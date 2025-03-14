package rainbow.realtime.common.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @author rainbow
 * @time 2025-03-13 16:57
 * @description 旁路缓存：
 * 思路：所有请求优先访问缓存，若缓存命中，直接获得数据返回给请求者。如果未命中则查询数据库，获取结果后，将其返回并写入缓存以备后续请求使用。
 * 选型：
 * 状态： 性能很好，维护性差
 * redis：性能不错，维护性好
 * 关于redis的一些设置：
 * key：        维度表名：主键值
 * type：       string
 * expire：     1day    避免冷数据常驻内存
 * 注意：维度数据发生变化直接清除缓存
 */
public class RedisUtil {

    private static final JedisPool jedisPool;

    static {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMinIdle(5);
        jedisPoolConfig.setMaxTotal(100);
        jedisPoolConfig.setMaxIdle(5);
        jedisPoolConfig.setBlockWhenExhausted(true);
        jedisPoolConfig.setMaxWaitMillis(2000);
        jedisPoolConfig.setTestOnBorrow(true);
        jedisPool = new JedisPool(jedisPoolConfig, "hadoop103", 6379, 10000);
    }

    //获取jedis
    public static Jedis getJedis() {
        System.out.println("~~~~~获取jedis客户端~~~~~");
        return jedisPool.getResource();
    }

    //关闭jedis
    public static void closeJedis(Jedis jedis) {
        System.out.println("~~~~~关闭jedis客户端~~~~~");
        if (jedis != null) {
            jedis.close();
        }
    }

    /**
     * 获取到 redis 的异步连接
     *
     * @return 异步链接对象
     */
    public static StatefulRedisConnection<String, String> getRedisAsyncConnection() {
        System.out.println("~~~~~获取异步Redis客户端~~~~~");
        RedisClient redisClient = RedisClient.create("redis://hadoop103:6379/0");
        return redisClient.connect();
    }

    /**
     * 关闭 redis 的异步连接
     *
     * @param asyncRedisConn 异步链接对象
     */
    public static void closeRedisAsyncConnection(StatefulRedisConnection<String, String> asyncRedisConn) {
        if (asyncRedisConn != null && asyncRedisConn.isOpen()) {
            System.out.println("~~~~~关闭异步Redis连接~~~~~");
            asyncRedisConn.close();
        }
    }


    /**
     * 异步从redis取数据
     */
    public static JSONObject readDimAsync(StatefulRedisConnection<String, String> asyncRedisConn, String tableName, String id) {
        RedisAsyncCommands<String, String> asyncCommands = asyncRedisConn.async();
        try {
            String dimJsonStr = asyncCommands.get(getKey(tableName, id)).get();
            if (StringUtils.isNotEmpty(dimJsonStr)) {
                return JSON.parseObject(dimJsonStr);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    /**
     * 异步往redis写数据
     */
    public static void writeDimAsync(StatefulRedisConnection<String, String> asyncRedisConn, String tableName, String id, JSONObject dimJsonObj) {
        RedisAsyncCommands<String, String> asyncCommands = asyncRedisConn.async();
        asyncCommands.setex(getKey(tableName, id), 3600 * 24, dimJsonObj.toJSONString());
    }

    /**
     * 从redis取数据
     */
    public static JSONObject readDim(Jedis jedis, String tableName, String id) {
        String key = getKey(tableName, id);
        //根据key到redis中获取维度数据
        String dimJsonStr = jedis.get(key);
        if (StringUtils.isNotEmpty(dimJsonStr)) {
            return JSON.parseObject(dimJsonStr);
        }
        return null;
    }

    //

    /**
     * 往redis写数据
     */
    public static void writeDim(Jedis jedis, String tableName, String id, JSONObject dimJsonObj) {
        String key = getKey(tableName, id);
        //将维度数据写入redis
        jedis.setex(key, 3600 * 24, dimJsonObj.toJSONString());
    }

    public static String getKey(String tableName, String id) {
        return tableName + ":" + id;
    }

    public static void main(String[] args) {
        Jedis jedis = getJedis();
        String pong = jedis.ping();
        System.out.println(pong);
        closeJedis(jedis);
    }
}
