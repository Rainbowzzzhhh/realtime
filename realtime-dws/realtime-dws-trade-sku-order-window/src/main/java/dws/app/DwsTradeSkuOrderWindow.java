package dws.app;

import com.alibaba.fastjson.JSONObject;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.AsyncConnection;
import rainbow.realtime.common.Base.BaseApp;
import rainbow.realtime.common.Base.TradeSkuOrderBean;
import rainbow.realtime.common.constant.Constant;
import rainbow.realtime.common.function.BeanToJsonStrMapFunction;
import rainbow.realtime.common.function.DimAsyncFunction;
import rainbow.realtime.common.util.DateFormatUtil;
import rainbow.realtime.common.util.FlinkSinkUtil;
import rainbow.realtime.common.util.HBaseUtil;
import rainbow.realtime.common.util.RedisUtil;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * @author rainbow
 * @time 2025-03-12 23:28
 * @description sku粒度下单业务过程聚合统计
 * 维度：sku
 * 度量：原始金额，优惠券减免金额，活动减免金额，实付金额
 * 开发流程：
 * 基本环境准备
 * 检查点相关设置
 * 从kafka的下单事实表中读取数据
 * 空消息的处理并将流中的数据类型进行转换
 * 去重：为什么产生重复数据？我们从下单事实表读取数据的，下单事实表有订单表，订单明细表，订单明细活动表，订单明细优惠券表四张表组成，订单表和订单明细表内连接，和订单明细活动表，订单明细优惠券表为左外连接，右表数据后到会产生3条数据
 * 去重前：按照唯一键分组
 * 方案1：状态+定时器：缺点：时效性差   优点：无数据冗余
 * 方案2：状态+抵消：缺点：有数据冗余   优点：时效性好
 * 指定watermark以及提取事件时间的字段
 * 再次对流中的数据进行类型转换   jsonObj-》实体类对象
 * 开窗
 * 聚合计算
 * 维度关联
 * 优化1：旁路缓存
 * 优化2：异步io
 */
public class DwsTradeSkuOrderWindow extends BaseApp {

    public static void main(String[] args) {
        new DwsTradeSkuOrderWindow().start(
                10029,
                4,
                "dws_trade_sku_order_window",
                Constant.TOPIC_DWD_TRADE_ORDER_DETAIL
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        //TODO 1.过滤空消息（通常flink自动过滤），并对流中的数据类型转换
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) {
                        if (jsonStr != null) {
                            out.collect(JSONObject.parseObject(jsonStr));
                        }
                    }
                }
        );

        //TODO 2.按照唯一键（订单明细id）进行分组
        KeyedStream<JSONObject, String> orderDetailKeyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getString("id"));

        //TODO 3.去重
        //method1：状态+定时器    缺点：时效性变差        优点：出现重复只会发送一条数据，无数据膨胀
        /*
        SingleOutputStreamOperator<JSONObject> distinctDS = orderDetailKeyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    private ValueState<JSONObject> lastJsonObjState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<JSONObject> lastJsonObjStateDescriptor
                                = new ValueStateDescriptor<>("lastJsonObjState", JSONObject.class);
                        lastJsonObjState = getRuntimeContext().getState(lastJsonObjStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        //获取上次的jsonObj
                        JSONObject lastJsonObj = lastJsonObjState.value();
                        if (lastJsonObj == null) {
                            //说明未重复 将当前接受到的数据放进状态中，并注册5s的定时器
                            lastJsonObjState.update(jsonObj);
                            long currentProcessingTime = ctx.timerService().currentProcessingTime();
                            ctx.timerService().registerProcessingTimeTimer(currentProcessingTime + 5000L);
                        } else {
                            //说明重复  用当前数据的聚合时间和状态中的数据聚合时间进行比较，将时间大的放到状态中
                            String lastTs = lastJsonObj.getString("");
                            String curTs = jsonObj.getString("");
                            if (curTs.compareTo(lastTs) >= 0) {
                                lastJsonObjState.update(jsonObj);
                                long currentProcessingTime = ctx.timerService().currentProcessingTime();
                                ctx.timerService().registerProcessingTimeTimer(currentProcessingTime + 5000L);
                            }
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, JSONObject, JSONObject>.OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                        //被执行时将状态的数据发送到下游，并清除状态
                        JSONObject jsonObj = lastJsonObjState.value();
                        out.collect(jsonObj);
                        lastJsonObjState.clear();
                    }
                }
        );
        */

        //method2：状态+抵消     优点：时效性好     缺点：如果出现重复，需要向下游传递三条数据，多出了两条，数据膨胀
        SingleOutputStreamOperator<JSONObject> distinctDS = orderDetailKeyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    private ValueState<JSONObject> lastJsonObjState;

                    @Override
                    public void open(Configuration parameters) {
                        ValueStateDescriptor<JSONObject> lastJsonObjStateDescriptor
                                = new ValueStateDescriptor<>("lastJsonObjState", JSONObject.class);
                        lastJsonObjStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10)).build());
                        lastJsonObjState = getRuntimeContext().getState(lastJsonObjStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        JSONObject lastJsonObj = lastJsonObjState.value();
                        if (lastJsonObj != null) {
                            //说明重复了，将发送到下游的数据，影响度量值的字段取反再传递到下游
                            //{"id":"12662","order_id":"6537","user_id":"405","sku_id":"1",
                            // "sku_name":"","province_id":"18","activity_id":"1","activity_rule_id":"1",
                            // "coupon_id":null,"date_id":"2025-03-08","create_time":"2025-03-08 00:37:17",
                            // "sku_num":"1","split_original_amount":"6999.0000","split_activity_amount":"500.0",
                            // "split_coupon_amount":"0.0","split_total_amount":"6499.0","ts":1741401643}
                            lastJsonObj.put("split_original_amount", "-" + lastJsonObj.getString("split_original_amount"));
                            lastJsonObj.put("split_activity_amount", "-" + lastJsonObj.getString("split_activity_amount"));
                            lastJsonObj.put("split_coupon_amount", "-" + lastJsonObj.getString("split_coupon_amount"));
                            lastJsonObj.put("split_total_amount", "-" + lastJsonObj.getString("split_total_amount"));
                            out.collect(lastJsonObj);
                        }
                        lastJsonObjState.update(jsonObj);
                        out.collect(jsonObj);
                    }
                }
        );

        //TODO 4.指定watermark以及提取事件时间字段
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = distinctDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                (SerializableTimestampAssigner<JSONObject>) (jsonObj, recordTimestamp) -> jsonObj.getLong("ts") * 1000
                        )
        );

        //TODO 5.再次对流中数据进行类型转换 jsonObj->统计的实体类对象
        SingleOutputStreamOperator<TradeSkuOrderBean> beanDS = withWatermarkDS.map(
                (MapFunction<JSONObject, TradeSkuOrderBean>) jsonObj -> TradeSkuOrderBean.builder()
                        .skuId(jsonObj.getString("sku_id"))
                        .originalAmount(jsonObj.getBigDecimal("split_original_amount"))
                        .couponReduceAmount(jsonObj.getBigDecimal("split_coupon_amount"))
                        .activityReduceAmount(jsonObj.getBigDecimal("split_activity_amount"))
                        .orderAmount(jsonObj.getBigDecimal("split_total_amount"))
                        .ts(jsonObj.getLong("ts") * 1000)
                        .build()
        );

        //TODO 6.分组
        KeyedStream<TradeSkuOrderBean, String> skuIdKeyedDS = beanDS.keyBy(TradeSkuOrderBean::getSkuId);

        //TODO 7.开窗     window对各组独立进行开窗，windowAll对整个流进行开窗
        WindowedStream<TradeSkuOrderBean, String, TimeWindow> windowDS = skuIdKeyedDS.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));

        //TODO 8.聚合
        SingleOutputStreamOperator<TradeSkuOrderBean> reduceDS = windowDS.reduce(
                (ReduceFunction<TradeSkuOrderBean>) (value1, value2) -> {
                    value1.setOriginalAmount(value1.getOriginalAmount().add(value2.getOriginalAmount()));
                    value1.setActivityReduceAmount(value1.getActivityReduceAmount().add(value2.getActivityReduceAmount()));
                    value1.setCouponReduceAmount(value1.getCouponReduceAmount().add(value2.getCouponReduceAmount()));
                    value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                    return value1;
                },
                new ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>.Context context, Iterable<TradeSkuOrderBean> elements, Collector<TradeSkuOrderBean> out) {
                        TradeSkuOrderBean orderBean = elements.iterator().next();
                        String stt = DateFormatUtil.tsToDateTime(context.window().getStart());
                        String edt = DateFormatUtil.tsToDateTime(context.window().getEnd());
                        String curDate = DateFormatUtil.tsToDate(context.window().getStart());
                        orderBean.setStt(stt);
                        orderBean.setEdt(edt);
                        orderBean.setCurDate(curDate);
                        out.collect(orderBean);
                    }
                }
        );
        //TODO 9.关联sku维度
        //维度关联的最基本的实现方式
        /*SingleOutputStreamOperator<TradeSkuOrderBean> withSkuInfoDS = reduceDS.map(
                new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
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
                    public TradeSkuOrderBean map(TradeSkuOrderBean orderBean) throws Exception {
                        //根据流中的对象获取要关联的维度的主键
                        String skuId = orderBean.getSkuId();
                        //根据维度的主键到Hbase维度表中获取对应的维度对象
                        //id,spu_id,price,sku_name,sku_desc,weight,tm_id,category3_id,sku_default_img,is_sale,create_time
                        JSONObject skuInfoJsonObj = HBaseUtil.getRow(hbaseConn, Constant.HBASE_NAMESPACE, "dim_sku_info", skuId, JSONObject.class);
                        //将维度对象相关的维度属性补充到流中的对象上
                        orderBean.setSkuName(skuInfoJsonObj.getString("sku_name"));
                        orderBean.setSpuId(skuInfoJsonObj.getString("spu_id"));
                        orderBean.setCategory3Id(skuInfoJsonObj.getString("category3_id"));
                        orderBean.setTrademarkId(skuInfoJsonObj.getString("tm_id"));
                        return orderBean;
                    }
                }
        );*/

        //优化1：旁路缓存
        /*
        SingleOutputStreamOperator<TradeSkuOrderBean> withSkuInfoDS = reduceDS.map(
                new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
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
                    public TradeSkuOrderBean map(TradeSkuOrderBean orderBean) {
                        //根据流中的对象获取要关联的维度的主键
                        String skuId = orderBean.getSkuId();
                        //根据维度的主键，先到redis中查询维度
                        JSONObject dimJsonObj = RedisUtil.readDim(jedis, "dim_sku_info", skuId);
                        if (dimJsonObj != null) {
                            //如果在redis中找到了对象的维度数据，直接作为查询结果返回
                            System.out.println("~~~从redis找到维度数据~~~");
                        } else {
                            //如果没找到，发送请求到Hbase中查询对应维度
                            System.out.println("~~~从Hbase找到维度数据~~~");
                            dimJsonObj = HBaseUtil.getRow(hbaseConn, Constant.HBASE_NAMESPACE, "dim_sku_info", skuId, JSONObject.class);
                            if (dimJsonObj != null) {
                                //将查询到的数据写到redis缓存起来
                                RedisUtil.writeDim(jedis, "dim_sku_info", skuId, dimJsonObj);
                            } else {
                                System.out.println("~~~未找到要关联的维度数据~~~");
                            }
                        }

                        //将维度对象相关的维度属性补充到流中对象上
                        if (dimJsonObj != null) {
                            orderBean.setSkuName(dimJsonObj.getString("sku_name"));
                            orderBean.setSpuId(dimJsonObj.getString("spu_id"));
                            orderBean.setCategory3Id(dimJsonObj.getString("category3_id"));
                            orderBean.setTrademarkId(dimJsonObj.getString("tm_id"));
                        }

                        return orderBean;
                    }
                }
        );
        */

        //优化2：异步IO [un]orderedWait()是否保证流中数据的顺序，等于map操作
        SingleOutputStreamOperator<TradeSkuOrderBean> withSkuInfoDS = AsyncDataStream.unorderedWait(
                reduceDS,
                //如何发送异步请求
                new RichAsyncFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
                    private AsyncConnection hbaseAsyncConn;
                    private StatefulRedisConnection<String, String> redisAsyncConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseAsyncConn = HBaseUtil.getHBaseAsyncConnection();
                        redisAsyncConn = RedisUtil.getRedisAsyncConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeAsyncHbaseConnection(hbaseAsyncConn);
                        RedisUtil.closeRedisAsyncConnection(redisAsyncConn);
                    }

                    @Override
                    public void asyncInvoke(TradeSkuOrderBean orderBean, ResultFuture<TradeSkuOrderBean> resultFuture) throws Exception {
                        //根据当前流中对象获取要关联的维度的主键
                        String skuId = orderBean.getSkuId();
                        //根据维度的主键到redis获取维度数据
                        JSONObject dimJsonObj = RedisUtil.readDimAsync(redisAsyncConn, "dim_sku_info", skuId);
                        if (dimJsonObj == null) {
                            //若未找到则在Hbase里面找，并写入redis
                            dimJsonObj = HBaseUtil.readDimAsync(hbaseAsyncConn, Constant.HBASE_NAMESPACE, "dim_sku_info", skuId);
                            if (dimJsonObj != null) {
                                RedisUtil.writeDimAsync(redisAsyncConn, "dim_sku_info", skuId, dimJsonObj);
                            }
                        }
                        if (dimJsonObj != null) {
                            orderBean.setSkuName(dimJsonObj.getString("sku_name"));
                            orderBean.setSpuId(dimJsonObj.getString("spu_id"));
                            orderBean.setCategory3Id(dimJsonObj.getString("category3_id"));
                            orderBean.setTrademarkId(dimJsonObj.getString("tm_id"));
                        }
                        resultFuture.complete(Collections.singleton(orderBean));
                    }
                },
                120,
                TimeUnit.SECONDS
        );

        //withSkuInfoDS.print();
        //TODO 10.关联spu维度
        SingleOutputStreamOperator<TradeSkuOrderBean> withSpuInfoDS = AsyncDataStream.unorderedWait(
                withSkuInfoDS,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public void addDims(TradeSkuOrderBean obj, JSONObject dimJsonObj) {
                        obj.setSpuName(dimJsonObj.getString("spu_name"));
                    }

                    @Override
                    public String getTableName() {
                        return "dim_spu_info";
                    }

                    @Override
                    public String getRowKey(TradeSkuOrderBean obj) {
                        return obj.getSpuId();
                    }
                },
                120,
                TimeUnit.SECONDS
        );

        //TODO 11.关联tm维度
        SingleOutputStreamOperator<TradeSkuOrderBean> withTmInfoDS = AsyncDataStream.unorderedWait(
                withSpuInfoDS,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public void addDims(TradeSkuOrderBean obj, JSONObject dimJsonObj) {
                        obj.setTrademarkName(dimJsonObj.getString("tm_name"));
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_trademark";
                    }

                    @Override
                    public String getRowKey(TradeSkuOrderBean obj) {
                        return obj.getTrademarkId();
                    }
                },
                120,
                TimeUnit.SECONDS
        );
        //TODO 12关联category3维度
        SingleOutputStreamOperator<TradeSkuOrderBean> withCategory3InfoDS = AsyncDataStream.unorderedWait(
                withTmInfoDS,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public void addDims(TradeSkuOrderBean obj, JSONObject dimJsonObj) {
                        obj.setCategory3Name(dimJsonObj.getString("name"));
                        obj.setCategory2Id(dimJsonObj.getString("category2_id"));
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_category3";
                    }

                    @Override
                    public String getRowKey(TradeSkuOrderBean obj) {
                        return obj.getCategory3Id();
                    }
                },
                120,
                TimeUnit.SECONDS
        );

        //TODO 13.关联category2维度
        SingleOutputStreamOperator<TradeSkuOrderBean> withCategory2InfoDS = AsyncDataStream.unorderedWait(
                withCategory3InfoDS,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public void addDims(TradeSkuOrderBean obj, JSONObject dimJsonObj) {
                        obj.setCategory2Name(dimJsonObj.getString("name"));
                        obj.setCategory1Id(dimJsonObj.getString("category1_id"));
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_category2";
                    }

                    @Override
                    public String getRowKey(TradeSkuOrderBean obj) {
                        return obj.getCategory2Id();
                    }
                },
                120,
                TimeUnit.SECONDS
        );

        //TODO 14.关联category1维度
        SingleOutputStreamOperator<TradeSkuOrderBean> withCategory1InfoDS = AsyncDataStream.unorderedWait(
                withCategory2InfoDS,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public void addDims(TradeSkuOrderBean obj, JSONObject dimJsonObj) {
                        obj.setCategory1Name(dimJsonObj.getString("name"));
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_category1";
                    }

                    @Override
                    public String getRowKey(TradeSkuOrderBean obj) {
                        return obj.getCategory1Id();
                    }
                },
                120,
                TimeUnit.SECONDS
        );

        //TODO 15.将结果写到doris
        withCategory1InfoDS.print();
        withCategory1InfoDS
                .map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_trade_sku_order_window"));
    }
}


























