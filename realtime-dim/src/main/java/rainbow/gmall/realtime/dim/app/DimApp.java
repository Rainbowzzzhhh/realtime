package rainbow.gmall.realtime.dim.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;
import rainbow.gmall.realtime.dim.function.HbaseSinkFunction;
import rainbow.gmall.realtime.dim.function.TableProcessFunction;
import rainbow.realtime.common.Base.BaseApp;
import rainbow.realtime.common.bean.TableProcessDim;
import rainbow.realtime.common.constant.Constant;
import rainbow.realtime.common.util.FlinkSourceUtil;
import rainbow.realtime.common.util.HBaseUtil;


/**
 * @author rainbow
 * @time 2024-12-29 2:21 PM
 * @description dim维度层的处理
 * 需要启动的进程：zk、kafka、maxwell、hdfs、Hbase、DimApp
 * 开发流程总结：
 * 基本环境准备
 * 检查点相关设置
 * 从kafka读取数据
 * 对流中的数据进行类型转换并etl jsonStr->jsonObj
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 * 使用FlinkCDC读取配置表中的配置信息
 * 对读取到的配置流数据进行类型转换
 * 根据当前配置信息到Hbase中执行建表或者删除操作
 * op=d    删除
 * op=c,r  建表
 * op=u    先删除再新建
 * 对配置流数据进行广播--broadcast
 * 关联主流业务数据以及广播流配置数据--connect
 * 对关联后的数据进行处理--process
 * new TableProcessFunction extends BroadcastProcessFunction{
 * open:将配置信息预加载到程序，避免主流数据先到，广播流数据后到，丢失数据的情况
 * processElement:
 * 获取操作的表的表名
 * 根据表名到广播状态和configMap中获取对应的配置信息，如果配置信息不为空，说明是维度，将维度数据发送到下游
 * Tuple2<dataJsonObj,配置对象>
 * 在向下游发送数据之前，过滤掉了不需要传递的属性，并补充了操作类型
 * processBroadcastElement:对广播流数据进行处理
 * op=d    将配置信息从configMap删除
 * op!=d   将配置信息放到广播状态和configMap
 * }
 * 将流中的数据同步到hbase中
 * new HbaseSinkFunction extends RichSinkFunction{
 * invoke:
 * type = delete   从hbase表删除数据
 * type != delete  从hbase表put数据
 * }
 * 优化：
 * 抽取FlinkSourceUtil
 * 抽取TableProcessFunction HbaseProcessFunction
 * 抽取基类
 * 执行流程：
 * 启动时，将配置表的配置信息加载到configMap中
 * 修改品牌维度
 * binlog会将修改操作记录下来
 * maxwell会从binlog中获取修改的信息，并封装为json格式字符串发送到kafka的topic_db主题
 * DimApp会从topic_db中读取数据并对其进行处理
 * 根据当前的数据的表名判断是否为维度
 * 如果是维度的话，将维度数据传递到1下游
 * 将维度数据同步到hbase
 */
public class DimApp extends BaseApp {

    public static void main(String[] args) throws Exception {
        new DimApp().start(10001, 4, "dim_app", Constant.TOPIC_DB);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {

        // TODO 4 对流中的数据类型进行转换并进行简单的etl json->jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> out) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        String db = jsonObj.getString("database");
                        String type = jsonObj.getString("type");
                        String data = jsonObj.getString("data");

                        if ("gmall".equals(db)
                                && ("insert".equals(type)
                                || "update".equals(type)
                                || "delete".equals(type)
                                || "bootstrap-insert".equals(type))
                                && data != null
                                && data.length() > 2
                        ) {
                            out.collect(jsonObj);
                        }
                    }
                }
        );

        //jsonObjDS.print();

        // TODO 5.使用FlinkCDC读取MySQL中配置表的数据
        // 5.1 创建MySqlSource对象
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMysqlSource("gmall_config", "table_process_dim");

        // 5.2 读取数据 封装为流
        DataStreamSource<String> mySQLStrDS = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .setParallelism(1);

        //mySQLStrDS.print();

        // TODO 6.对配置流中的数据类型进行转换
        SingleOutputStreamOperator<TableProcessDim> tpDS = mySQLStrDS.map(
                (MapFunction<String, TableProcessDim>) jsonStr -> {
                    // 将数据转换成json对象
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    //System.out.println("------------------------" + jsonObj.toString());
                    String op = jsonObj.getString("op");
                    TableProcessDim tableProcessDim = null;
                    if ("d".equals(op)) {
                        //删除，从before去获取删除的配置信息
                        tableProcessDim = jsonObj.getObject("before", TableProcessDim.class);
                    } else {
                        //其他操作，从after获取配置信息
                        tableProcessDim = jsonObj.getObject("after", TableProcessDim.class);
                    }

                    tableProcessDim.setOp(op);

                    return tableProcessDim;
                }
        ).setParallelism(1);

        //tpDS.print();

        // TODO 7.根据配置表中的配置信息到HBase中进行建/删表操作
        tpDS = tpDS.map(
                new RichMapFunction<TableProcessDim, TableProcessDim>() {

                    private Connection hbaseConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn = HBaseUtil.getHBaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeHBaseConn(hbaseConn);
                    }

                    @Override
                    public TableProcessDim map(TableProcessDim tp) throws Exception {
                        // 获取对配置表的进行操作的类型
                        String op = tp.getOp();
                        // 获取Hbase中维度表的表名
                        String sinkTable = tp.getSinkTable();
                        // 获取Hbase中建表的列族
                        String[] sinkFamilies = tp.getSinkFamily().split(",");

                        if ("d".equals(op)) {
                            // 删除
                            HBaseUtil.dropHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable);
                        } else if ("r".equals(op) || "c".equals(op)) {
                            // 从配置表中读取或添加数据，执行建表操作
                            HBaseUtil.createHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable, sinkFamilies);
                        } else {
                            // 对配置表信息进行修改，先删除hbase的表删除再创建新表
                            HBaseUtil.dropHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable);
                            HBaseUtil.createHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable, sinkFamilies);
                        }
                        return tp;
                    }
                }
        ).setParallelism(1);

        // TODO 8.将配置流中的配置信息进行广播--broadcast
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor
                = new MapStateDescriptor<String, TableProcessDim>("mapStateDescriptor", String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastDS = tpDS.broadcast(mapStateDescriptor);

        // TODO 9.将配置流中的配置信息广播后，广播流和主流进行连接--connect
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectDS = jsonObjDS.connect(broadcastDS);

        // TODO 10.处理关联后的数据，判读是否为维度数据
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDS =
                connectDS.process(new TableProcessFunction(mapStateDescriptor));

        // TODO 11.将维度数据同步到HBase中
        dimDS.addSink(new HbaseSinkFunction());
        dimDS.print();
    }
}
