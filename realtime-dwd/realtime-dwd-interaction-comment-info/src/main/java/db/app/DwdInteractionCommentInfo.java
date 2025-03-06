package db.app;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import rainbow.realtime.common.constant.Constant;

/**
 * @author rainbow
 * @time 2025-03-06 21:44
 * @description ...
 */
public class DwdInteractionCommentInfo {
    public static void main(String[] args) {
        //TODO 1.环境准备
        //1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);
        //1.3 指定表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //TODO 2.检查点相关设置
        //2.1 开启检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        //2.2 设置检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(6000L);
        //2.3 设置状态取消后，检查点是否保留
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //2.4 设置两个检查点之间的最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        //2.5 设置重启策略
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30), Time.seconds(30)));
        //2.6 设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/ck");
        //2.7 设置操作hadoop用户
        System.setProperty("HADOOP_USER_NAME", "root");
        //TODO 3.从kafka的topic_db主题中读取数据 创建动态表   --kafka连接器
        tableEnv.executeSql(
                "   CREATE TABLE topic_db (\n" +
                        "       `database` string,\n" +
                        "       `table` string,\n" +
                        "       `type` string,\n" +
                        "       `ts` bigint,\n" +
                        "       `data` map<string,string>,\n" +
                        "       `old` map<string,string>,\n" +
                        "       proc_time as proctime()\n" +
                        "   ) WITH (\n" +
                        "       'connector' = 'kafka',\n" +
                        "       'topic' = 'topic_db',\n" +
                        "       'properties.bootstrap.servers' = 'hadoop102:9092',\n" +
                        "       'properties.group.id' = 'testGroup',\n" +
                        "       'scan.startup.mode' = 'latest-offset',\n" +
                        "       'format' = 'json'\n" +
                        "    )");

        //TODO 4.过滤出评论数据                            --where table = 'comment_info' type = insert
        Table commentInfo = tableEnv.sqlQuery(
                "   select  " +
                        "       `data`['id']    id,    " +
                        "       `data`['user_id']   user_id,  " +
                        "       `data`['sku_id']    sku_id,    " +
                        "       `data`['appraise']  appraise,    " +
                        "       `data`['comment_txt']   comment_txt,  " +
                        "       ts,     " +
                        "       proc_time   " +
                        "    from topic_db  " +
                        "    where `database`='gmall'   " +
                        "    and `table`='comment_info'     " +
                        "    and `type`='insert'    "
        );
        //commentInfo.execute().print();
        //将表对象注册进表执行环境中
        tableEnv.createTemporaryView("comment_info", commentInfo);

        //TODO 5.从Hbase中读取字典数据，创建动态表            --hbase连接器
        tableEnv.executeSql(
                "CREATE TABLE base_dic (                    \n" +
                        "   dic_code STRING,                        \n" +
                        "   info ROW<dic_name STRING>,              \n" +
                        "   PRIMARY KEY (dic_code) NOT ENFORCED     \n" +
                        ") WITH (                                   \n" +
                        "   'connector' = 'hbase-2.2',              \n" +
                        "   'table-name' = '" + Constant.HBASE_NAMESPACE + ":dim_base_dic', \n" +
                        "   'zookeeper.quorum' = 'hadoop102:2181,hadoop103:2181,hadoop104:2181', \n" +
                        "   'lookup.async' = 'true',                \n" +
                        "   'lookup.cache' = 'PARTIAL',             \n" +
                        "   'lookup.partial-cache.max-rows' = '500',\n" +
                        "   'lookup.partial-cache.expire-after-write' = '1 hour',\n" +
                        "   'lookup.partial-cache.expire-after-access' = '1 hour'\n" +
                        ")"
        );
        //tableEnv.executeSql("select * from base_dic").print();


        //TODO 6.将评论表和字典表进行关联                    --lookupJoin
        Table joinedTable = tableEnv.sqlQuery(
                "SELECT \n" +
                        "    c.id,\n" +
                        "    c.user_id,\n" +
                        "    c.sku_id,\n" +
                        "    c.appraise,\n" +
                        "    dic.dic_name appraise_name,\n" +
                        "    c.comment_txt,\n" +
                        "    c.ts\n" +
                        "FROM comment_info AS c\n" +
                        "JOIN base_dic FOR SYSTEM_TIME AS OF c.proc_time AS dic\n" +
                        "ON c.appraise = dic.dic_code"
        );
        joinedTable.execute().print();

        //TODO 7.将关联的结果写到kafka主题中                 --upsertKafka连接器
        //7.1 创建动态表和要写入的主题进行映射
        //7.2 写入
    }
}
