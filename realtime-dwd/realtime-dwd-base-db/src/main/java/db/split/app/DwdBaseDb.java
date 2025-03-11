package db.split.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import db.split.function.BaseTableProcessFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import rainbow.realtime.common.Base.BaseApp;
import rainbow.realtime.common.bean.TableProcessDwd;
import rainbow.realtime.common.constant.Constant;
import rainbow.realtime.common.util.FlinkSinkUtil;
import rainbow.realtime.common.util.FlinkSourceUtil;

/**
 * @author rainbow
 * @time 2025-03-08 23:02
 * @description 处理逻辑比较简单的事实表动态分流处理
 */
public class DwdBaseDb extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwdBaseDb().start(10019, 4, "dwd_base_db", Constant.TOPIC_DB);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        //TODO 对数据进行类型转换并进行简单的etl jsonStr->jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(jsonStr);
                    String type = jsonObject.getString("type");
                    if (!type.startsWith("bootstrap-")) {
                        collector.collect(jsonObject);
                    }
                } catch (Exception e) {
                    throw new RuntimeException("不是一个标准的json");
                }
            }
        });

        // jsonObjDS.print();

        //TODO 使用FlinkCDC读取配置表的配置信息
        //创建MysqlSource表
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMysqlSource("gmall_config", "table_process_dwd");
        //读取数据 封装为流
        DataStreamSource<String> mysqlStrDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source");
        //对流中的数据进行类型转化 jsonStr->实体对象
        SingleOutputStreamOperator<TableProcessDwd> tpDS = mysqlStrDS.map(
                new MapFunction<String, TableProcessDwd>() {
                    @Override
                    public TableProcessDwd map(String jsonStr) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(jsonStr);
                        String op = jsonObject.getString("op");
                        TableProcessDwd tp = null;
                        if ("d".equals(op)) {
                            //删除，从before获取
                            tp = jsonObject.getObject("before", TableProcessDwd.class);
                        } else {
                            //非删除，从after获取
                            tp = jsonObject.getObject("after", TableProcessDwd.class);
                        }
                        tp.setOp(op);
                        return tp;
                    }
                }
        );

        //tpDS.print();

        //TODO 对配置流进行广播 --broadcast
        MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor = new MapStateDescriptor<>("MapStateDescriptor", String.class, TableProcessDwd.class);
        BroadcastStream<TableProcessDwd> broadcastDS = tpDS.broadcast(mapStateDescriptor);

        //TODO 关联主流业务数据和广播流的配置数据 --connect
        BroadcastConnectedStream<JSONObject, TableProcessDwd> connectDS = jsonObjDS.connect(broadcastDS);

        //TODO 对关联后的数据进行处理  --process
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> splitDS = connectDS.process(new BaseTableProcessFunction(mapStateDescriptor));

        //TODO 将处理逻辑比较简单的事实表数据写到kafka的不同主题中
        splitDS.sinkTo(FlinkSinkUtil.getKafkaSink());
        splitDS.print();

    }
}
