package rainbow.gmall.realtime.dim.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import rainbow.realtime.common.bean.TableProcessDim;
import rainbow.realtime.common.constant.Constant;
import rainbow.realtime.common.util.JdbcUtil;

import java.sql.*;
import java.util.*;

/**
 * @author rainbow
 * @time 2025-03-04 13:55
 * @description ...
 */

// 非广播流，广播流，输出
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>> {

    private MapStateDescriptor<String, TableProcessDim> mapStateDescriptor;
    private Map<String, TableProcessDim> configMap = new HashMap<>();

    public TableProcessFunction(MapStateDescriptor<String, TableProcessDim> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //将配置表的信息预加载到程序configMap中
        Connection mysqlConnection = JdbcUtil.getMysqlConnection();
        String sql = "select * from gmall_config.table_process_dim";
        List<TableProcessDim> tableProcessDimList= JdbcUtil.queryList(mysqlConnection, sql, TableProcessDim.class, true);
        for (TableProcessDim tableProcessDim : tableProcessDimList) {
            configMap.put(tableProcessDim.getSourceTable(),tableProcessDim);
        }
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

    // 删除不需要的属性
    private static void deleteNotNeedColumns(JSONObject dataJsonObj, String sinkColumns) {
        List<String> columnList = Arrays.asList(sinkColumns.split(","));

        Set<Map.Entry<String, Object>> entrySet = dataJsonObj.entrySet();

        entrySet.removeIf(next -> !columnList.contains(next.getKey()));

    }
}
