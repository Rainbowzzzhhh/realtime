package db.split.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import rainbow.realtime.common.bean.TableProcessDwd;
import rainbow.realtime.common.util.JdbcUtil;

import java.sql.Connection;
import java.util.*;

/**
 * @author rainbow
 * @time 2025-03-09 01:18
 * @description ...
 */

public class BaseTableProcessFunction extends BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>> {

    private final MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor;

    private final Map<String, TableProcessDwd> configMap = new HashMap<>();

    public BaseTableProcessFunction(MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //将配置信息预加载
        Connection mysqlConnection = JdbcUtil.getMysqlConnection();
        List<TableProcessDwd> tableProcessDwdList = JdbcUtil.queryList(
                mysqlConnection,
                "select * from gmall_config.table_process_dwd",
                TableProcessDwd.class,
                true);
        for (TableProcessDwd tableProcessDwd : tableProcessDwdList) {
            String sourceTable = tableProcessDwd.getSourceTable();
            String sourceType = tableProcessDwd.getSourceType();
            String key = getKey(sourceTable, sourceType);
            configMap.put(key, tableProcessDwd);
        }
        JdbcUtil.closeConnection(mysqlConnection);
    }

    private static String getKey(String sourceTable, String sourceType) {
        return sourceTable + ":" + sourceType;
    }

    @Override
    //处理主流
    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.ReadOnlyContext readOnlyContext, Collector<Tuple2<JSONObject, TableProcessDwd>> collector) throws Exception {
        String table = jsonObject.getString("table");
        String type = jsonObject.getString("type");
        String key = getKey(table, type);
        //获取广播状态
        ReadOnlyBroadcastState<String, TableProcessDwd> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
        //根据key获取对应的配置信息
        TableProcessDwd tp = null;
        if ((tp = broadcastState.get(key)) != null || (tp = configMap.get(key)) != null) {
            //说明为需要动态分流处理的事实表数据
            JSONObject dataJsonObj = jsonObject.getJSONObject("data");
            //过滤无用字段
            String sinkColumns = tp.getSinkColumns();
            deleteNotNeedColumns(dataJsonObj, sinkColumns);
            //补充ts事件时间到data
            dataJsonObj.put("ts", jsonObject.getLong("ts"));
            collector.collect(Tuple2.of(dataJsonObj, tp));
        }
    }

    // 删除不需要的属性
    private static void deleteNotNeedColumns(JSONObject dataJsonObj, String sinkColumns) {
        List<String> columnList = Arrays.asList(sinkColumns.split(","));

        Set<Map.Entry<String, Object>> entrySet = dataJsonObj.entrySet();

        entrySet.removeIf(next -> !columnList.contains(next.getKey()));

    }

    @Override
    //处理广播流
    public void processBroadcastElement(TableProcessDwd tp, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.Context context, Collector<Tuple2<JSONObject, TableProcessDwd>> collector) throws Exception {
        //获取操作类型
        String op = tp.getOp();
        //获取广播状态
        BroadcastState<String, TableProcessDwd> broadcastState = context.getBroadcastState(mapStateDescriptor);
        //获取业务数据库表的表名和操作类型
        String sourceTable = tp.getSourceTable();
        String sourceType = tp.getSourceType();
        String key = getKey(sourceTable, sourceType);
        if ("d".equals(op)) {
            //删除，从广播状态和configMap中删除数据
            broadcastState.remove(key);
            configMap.remove(key);
        } else {
            //更新配置信息在广播状态和configMap
            broadcastState.put(key, tp);
            configMap.put(key, tp);
        }
    }
}
