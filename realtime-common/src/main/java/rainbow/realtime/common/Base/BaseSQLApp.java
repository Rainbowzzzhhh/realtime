package rainbow.realtime.common.Base;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import rainbow.realtime.common.constant.Constant;
import rainbow.realtime.common.util.SQLUtil;

/**
 * @author rainbow
 * @time 2025-03-06 21:44
 * @description FLinkSQL基类
 */
public abstract class BaseSQLApp {
    public void start(int port, int parallelism, String ckAndGroupId) {
        //TODO 1.环境准备
        //1.1 指定流处理环境
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, port);   //推荐

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        //1.2 设置并行度
        env.setParallelism(parallelism);
        //1.3 指定表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 2.检查点相关设置
//        //2.1 开启检查点
//        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//        //2.2 设置检查点超时时间
//        env.getCheckpointConfig().setCheckpointTimeout(6000L);
//        //2.3 设置状态取消后，检查点是否保留
//        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        //2.4 设置两个检查点之间的最小时间间隔
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
//        //2.5 设置重启策略
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30), Time.seconds(30)));
//        //2.6 设置状态后端
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/ck/" + ckAndGroupId);
//        //2.7 设置操作hadoop用户
//        System.setProperty("HADOOP_USER_NAME", "root");

        //TODO 3.业务处理逻辑
        handle(tableEnv);
    }

    public abstract void handle(StreamTableEnvironment tableEnv);

    //读取topic_db主题中的数据，创建动态表
    public void readOdsDb(StreamTableEnvironment tableEnv,String groupId) {
        tableEnv.executeSql(
                "   CREATE TABLE topic_db (\n" +
                        "       `database` string,                              \n" +
                        "       `table` string,                                 \n" +
                        "       `type` string,                                  \n" +
                        "       `ts` bigint,                                    \n" +
                        "       `data` map<string,string>,                      \n" +
                        "       `old` map<string,string>,                       \n" +
                        "       `pt`  as proctime(),                            \n" +
                        "       `et`  as to_timestamp_ltz(ts, 0),               \n" +
                        "        watermark for et as et - interval '3' second   \n" +
                        "   )" + SQLUtil.getKafkaDDL(Constant.TOPIC_DB, groupId)
        );
    }

    //从hbase中的字典表读数据，创建动态表
    public void readBaseDic(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql(
                "CREATE TABLE base_dic (                    \n" +
                        "   dic_code STRING,                        \n" +
                        "   info ROW<dic_name STRING>,              \n" +
                        "   PRIMARY KEY (dic_code) NOT ENFORCED     \n" +
                        ") " + SQLUtil.getHBaseDDL(Constant.HBASE_NAMESPACE, "dim_base_dic")
        );
    }
}
