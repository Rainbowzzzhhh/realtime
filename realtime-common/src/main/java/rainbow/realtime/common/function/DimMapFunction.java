package rainbow.realtime.common.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import rainbow.realtime.common.constant.Constant;
import rainbow.realtime.common.util.HBaseUtil;
import rainbow.realtime.common.util.RedisUtil;
import redis.clients.jedis.Jedis;


/**
 * @author rainbow
 * @time 2025-03-13 19:14
 * @description 维度关联旁路缓存优化抽取
 */

public abstract class DimMapFunction<T> extends RichMapFunction<T, T> implements DimFunction {
    private Connection hbaseConn;
    private Jedis jedis;

    @Override
    public void open(Configuration parameters) throws Exception {
        hbaseConn = HBaseUtil.getHBaseConnection();
        jedis = RedisUtil.getJedis();
    }

    @Override
    public void close() throws Exception {
        HBaseUtil.closeHBaseConn(hbaseConn);
        RedisUtil.closeJedis(jedis);
    }

    @Override
    public T map(T obj) throws Exception {
        //根据流中的对象获取要关联的维度的主键
        String key = getRowKey(obj);
        //根据维度的主键到redis中获取对应的维度对象
        JSONObject dimJsonObj = RedisUtil.readDim(jedis, getTableName(), key);

        if (dimJsonObj != null) {
            //如果在redis中找到了对象的维度数据，直接作为查询结果返回
            System.out.println("~~~从Redis找到" + getTableName() + "的数据" + key + "~~~");
        } else {
            //如果没找到，发送请求到Hbase中查询对应维度
            dimJsonObj = HBaseUtil.getRow(hbaseConn, Constant.HBASE_NAMESPACE, getTableName(), key, JSONObject.class);
            if (dimJsonObj != null) {
                //将查询到的数据写到redis缓存起来
                System.out.println("~~~从Hbase找到" + getTableName() + "的数据" + key + "~~~");
                RedisUtil.writeDim(jedis, getTableName(), key, dimJsonObj);
            } else {
                System.out.println("~~~没找到" + getTableName() + "的数据" + key + "~~~");
            }

        }
        //将维度对象相关的维度属性补充到流中对象上
        if (dimJsonObj != null) {
            addDims(obj, dimJsonObj);
        }

        return obj;
    }


}
