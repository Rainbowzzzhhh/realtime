package rainbow.gmall.realtime.dim.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.hadoop.hbase.client.Connection;
import rainbow.realtime.common.bean.TableProcessDim;
import rainbow.realtime.common.constant.Constant;
import rainbow.realtime.common.util.HBaseUtil;

/**
 * @author rainbow
 * @time 2025-03-04 14:03
 * @description ...
 */
public class HbaseSinkFunction extends RichSinkFunction<Tuple2<JSONObject, TableProcessDim>> {

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
    public void invoke(Tuple2<JSONObject, TableProcessDim> tuple2, SinkFunction.Context context) throws Exception {
        JSONObject jsonObj = tuple2.f0;
        TableProcessDim tableProcessDim = tuple2.f1;
        String type = jsonObj.getString("type");
        jsonObj.remove("type");

        String sinkTable = tableProcessDim.getSinkTable();
        String rowKey = jsonObj.getString(tableProcessDim.getSinkRowKey());

        //判断对HBASE操作类型
        if (type.equals("delete")) {
            HBaseUtil.delRow(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable, rowKey);
        } else {
            HBaseUtil.putRow(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable, rowKey, tableProcessDim.getSinkFamily(), jsonObj);
        }
    }
}
