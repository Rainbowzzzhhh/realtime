package rainbow.gmall.realtime.dim.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;
import rainbow.realtime.common.bean.TableProcessDim;
import rainbow.realtime.common.constant.Constant;
import rainbow.realtime.common.util.FlinkSourceUtil;
import rainbow.realtime.common.util.HBaseUtil;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.*;

/**
 * @author rainbow
 * @time 2024-12-29 2:21 PM
 * @description dim维度层的处理
 * 需要启动的进程：zk、kafka、maxwell、hdfs、Hbase、DimApp
 */
public class DimApp {
    public static void main(String[] args) throws Exception {
        // TODO 1.获取执行环境
        // 1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1.2 设置并行度
        env.setParallelism(4);

        // TODO 2.检查点相关设置

        // 2.1 开启检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);

        // 2.2 设置检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000L);

        // 2.3 设置job取消后检查点是否保留
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 2.4 设置两个检查点的最小间隔时间
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);

        // 2.5 设置重启策略
        // env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000L));
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30), Time.seconds(3)));

        // 2.6 设置状态后端以及检查点的存储路径
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/realtime/ck");

        // 2.7 设置操作hadoop用户
        System.setProperty("HADOOP_USER_NAME", "root");

        // TODO 3.从Kafka的topic_db读取业务数据，封装为流
        // 3.1 声明消费主题以及消费者组
        String groupId = "dim_app_group";

        // 3.2 创建消费者对象
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource(Constant.TOPIC_DB, groupId);

        // 3.3 消费数据 封装为流
        DataStreamSource<String> kafkaStrDS
                = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

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
        Properties props = new Properties();
        props.setProperty("useSSL", "false");
        props.setProperty("allowPublicKeyRetrieval", "true");

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .port(Constant.MYSQL_PORT)
                .databaseList("gmall_config")
                .tableList("gmall_config.table_process_dim")
                .username(Constant.MYSQL_USER_NAME)
                .password(Constant.MYSQL_PASSWORD)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .jdbcProperties(props)
                .build();

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

        tpDS.print();

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
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDS = connectDS.process(
                // 非广播流，广播流，输出
                new BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>() {

                    private Map<String, TableProcessDim> configMap = new HashMap<>();

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //将配置表的信息预加载到程序configMap中
                        Class.forName("com.mysql.cj.jdbc.Driver");
                        java.sql.Connection conn = DriverManager.getConnection(Constant.MYSQL_URL, Constant.MYSQL_USER_NAME, Constant.MYSQL_PASSWORD);
                        String sql = "select * from gmall_config.table_process_dim";
                        PreparedStatement ps = conn.prepareStatement(sql);
                        ResultSet rs = ps.executeQuery();
                        ResultSetMetaData metaData = rs.getMetaData();
                        //handle rs
                        while (rs.next()) {
                            //定义一个json对象接受遍历的数据
                            JSONObject jsonObj = new JSONObject();
                            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                                String columnName = metaData.getColumnName(i);
                                Object value = rs.getObject(i);
                                jsonObj.put(columnName, value);
                            }
                            TableProcessDim tableProcessDim = jsonObj.toJavaObject(TableProcessDim.class);
                            configMap.put(tableProcessDim.getSourceTable(), tableProcessDim);

                        }

                        rs.close();
                        ps.close();
                        conn.close();

                    }

                    // processElement：处理主流业务数据              根据维度表名，从广播流中获取维度表对象，根据维度表对象，
                    @Override
                    public void processElement(JSONObject jsonObject,
                                               BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.ReadOnlyContext readOnlyContext,
                                               Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {
                        String table = jsonObject.getString("table");
                        // 获取广播状态
                        ReadOnlyBroadcastState<String, TableProcessDim> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
                        // 根据表名到广播状态获取配置信息
                        TableProcessDim tableProcessDim = null;

                        if ((tableProcessDim = broadcastState.get(table)) != null ||
                                (tableProcessDim = configMap.get(table)) != null) {
                            // 如果获取到配置信息则为维度数据，将维度数据向下游传递(只需传递data内容)
                            JSONObject dataJsonObj = jsonObject.getJSONObject("data");

                            //先删除不需要的属性
                            String sinkColumns = tableProcessDim.getSinkColumns();
                            deleteNotNeedColumns(dataJsonObj, sinkColumns);

                            // 补充操作类型数据
                            String type = jsonObject.getString("type");
                            dataJsonObj.put("type", type);

                            out.collect(Tuple2.of(dataJsonObj, tableProcessDim));

                        }
                    }

                    // processBroadcastElement：处理广播流配置信息   将配置数据放到广播流中或者从广播状态中删除配置     k：配置表名 v：一个配置对象
                    @Override
                    public void processBroadcastElement(TableProcessDim tableProcessDim,
                                                        BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.Context context,
                                                        Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {
                        // 获取配置表操作类型
                        String op = tableProcessDim.getOp();
                        // 获取广播状态
                        BroadcastState<String, TableProcessDim> broadcastState = context.getBroadcastState(mapStateDescriptor);

                        if ("d".equals(op)) {
                            // 从配置表删除一条数据，将对象的配置信息删除
                            String sourceTable = tableProcessDim.getSourceTable();
                            broadcastState.remove(sourceTable);
                            configMap.remove(sourceTable);

                        } else {
                            // 添加一条数据，将对象的配置信息添加到广播状态中
                            broadcastState.put(tableProcessDim.getSourceTable(), tableProcessDim);
                            //可有可无 configMap.put(tableProcessDim.getSourceTable(), tableProcessDim);
                        }
                    }
                }
        );

        // TODO 11.将维度数据同步到HBase中
        dimDS.addSink(new RichSinkFunction<Tuple2<JSONObject, TableProcessDim>>() {

            private Connection hbaseConn;

            @Override
            public void open(Configuration parameters) throws Exception {
                hbaseConn = HBaseUtil.getHBaseConnection();
            }

            @Override
            public void close() throws Exception {
                HBaseUtil.closeHBaseConn(hbaseConn);
            }

            //将流数据写入HBASE
            @Override
            public void invoke(Tuple2<JSONObject, TableProcessDim> tuple2, Context context) throws Exception {
                JSONObject jsonObj = tuple2.f0;
                TableProcessDim tableProcessDim = tuple2.f1;
                String type = jsonObj.getString("type");
                jsonObj.remove("type");

                //判断对HBASE操作类型
                if (type.equals("delete")) {
                    HBaseUtil.delRow(hbaseConn, Constant.HBASE_NAMESPACE, tableProcessDim.getSinkTable(),
                            jsonObj.getString(tableProcessDim.getSinkRowKey()));
                } else {
                    HBaseUtil.putRow(hbaseConn, Constant.HBASE_NAMESPACE, tableProcessDim.getSinkTable(),
                            jsonObj.getString(tableProcessDim.getSinkRowKey()), tableProcessDim.getSinkFamily(), jsonObj);
                }
            }
        });

        env.execute();
    }

    // 删除不需要的属性
    private static void deleteNotNeedColumns(JSONObject dataJsonObj, String sinkColumns) {
        List<String> columnList = Arrays.asList(sinkColumns.split(","));

        Set<Map.Entry<String, Object>> entrySet = dataJsonObj.entrySet();

        entrySet.removeIf(next -> !columnList.contains(next.getKey()));

    }
}
