package rainbow.realtime.common.util;

import rainbow.realtime.common.constant.Constant;

/**
 * @author rainbow
 * @time 2025-03-07 14:17
 * @description ...
 */
public class SQLUtil {
    //获取kafka连接器的连接属性
    public static String getKafkaDDL(String topic, String groupId) {
        return " WITH (\n" +
                "       'connector' = 'kafka',\n" +
                "       'topic' = '" + topic + "',\n" +
                "       'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',\n" +
                "       'properties.group.id' = '" + groupId + "',\n" +
                "       'scan.startup.mode' = 'latest-offset',\n" +
                "       'format' = 'json'\n" +
                "       )";
    }

    //获取hbase连接器的连接属性
    public static String getHBaseDDL(String nameSpace, String tableName) {
        return "WITH (                                                                      \n" +
                "   'connector' = 'hbase-2.2',                                              \n" +
                "   'table-name' = '" + nameSpace + ":" + tableName + "',         \n" +
                "   'zookeeper.quorum' = 'hadoop102:2181,hadoop103:2181,hadoop104:2181',    \n" +
                "   'lookup.async' = 'true',                                                \n" +
                "   'lookup.cache' = 'PARTIAL',                                             \n" +
                "   'lookup.partial-cache.max-rows' = '500',                                \n" +
                "   'lookup.partial-cache.expire-after-write' = '1 hour',                   \n" +
                "   'lookup.partial-cache.expire-after-access' = '1 hour'                   \n" +
                ")";
    }

    //获取upsert kafka连接器
    public static String getUpsertKafkaDDL(String topic) {
        return "WITH(\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = '" + topic + "',\n" +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                "   )";
    }
}
