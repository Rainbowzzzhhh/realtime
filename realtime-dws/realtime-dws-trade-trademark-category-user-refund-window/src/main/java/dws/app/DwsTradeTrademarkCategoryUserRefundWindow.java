package dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import rainbow.realtime.common.Base.BaseApp;
import rainbow.realtime.common.bean.TradeTrademarkCategoryUserRefundBean;
import rainbow.realtime.common.constant.Constant;
import rainbow.realtime.common.function.BeanToJsonStrMapFunction;
import rainbow.realtime.common.function.DimAsyncFunction;
import rainbow.realtime.common.util.DateFormatUtil;
import rainbow.realtime.common.util.FlinkSinkUtil;


import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * @author rainbow
 * @time 2025-03-14 18:01
 * @description 从 Kafka 读取退单明细数据，
 * 关联与分组相关的维度信息后分组，
 * 统计各分组各窗口的订单数和订单金额，
 * 补充与分组无关的维度信息，
 * 将数据写入 Doris 交易域品牌-品类-用户粒度退单各窗口汇总表
 */

public class DwsTradeTrademarkCategoryUserRefundWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTradeTrademarkCategoryUserRefundWindow().start(
                10031,
                4,
                "dws_trade_trademark_category_user_refund_window",
                Constant.TOPIC_DWD_TRADE_ORDER_REFUND
        );
    }

    //{"id":"295","user_id":"3131","order_id":"8577","sku_id":"11","province_id":"13",
// "date_id":"2025-03-08","create_time":"2025-03-08 23:07:46","refund_type_code":"1501",
// "refund_type_name":"仅退款","refund_reason_type_code":"1301","refund_reason_type_name":"质量问题",
// "refund_reason_txt":"退款原因具体：0036226899","refund_num":"1","refund_amount":"8197.0","ts":1741442117}
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        //2）转换数据结构
        //JSONObject转换为实体类TradeTrademarkCategoryUserRefundBean。
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> beanDS = kafkaStrDS.map(
                new MapFunction<String, TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public TradeTrademarkCategoryUserRefundBean map(String jsonStr) {
                        JSONObject JsonObj = JSON.parseObject(jsonStr);
                        return TradeTrademarkCategoryUserRefundBean.builder()
                                .userId(JsonObj.getString("user_id"))
                                .skuId(JsonObj.getString("sku_id"))
                                .ts(JsonObj.getLong("ts") * 1000)
                                .orderIdSet(new HashSet<>(Collections.singleton(JsonObj.getString("order_id"))))
                                .build();

                    }
                }
        );
        //3）补充与分组相关的维度信息
        //（1）关联sku_info表
        //（2）获取tm_id，category3_id。
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> withSkuInfoDS = AsyncDataStream.unorderedWait(
                beanDS,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public void addDims(TradeTrademarkCategoryUserRefundBean obj, JSONObject dimJsonObj) {
                        //id,spu_id,price,sku_name,sku_desc,weight,tm_id,category3_id,sku_default_img,is_sale,create_time
                        obj.setTrademarkId(dimJsonObj.getString("tm_id"));
                        obj.setCategory3Id(dimJsonObj.getString("category3_id"));
                    }

                    @Override
                    public String getTableName() {
                        return "dim_sku_info";
                    }

                    @Override
                    public String getRowKey(TradeTrademarkCategoryUserRefundBean obj) {
                        return obj.getSkuId();
                    }
                },
                120,
                TimeUnit.SECONDS
        );
        //4）设置水位线
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> withWatermarkDS
                = withSkuInfoDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<TradeTrademarkCategoryUserRefundBean>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<TradeTrademarkCategoryUserRefundBean>() {
                                    @Override
                                    public long extractTimestamp(TradeTrademarkCategoryUserRefundBean element, long recordTimestamp) {
                                        return element.getTs();
                                    }
                                }
                        )
        );

        //withWatermarkDS.print("withWatermarkDS");

        //5）分组、开窗、聚合
        //按照维度信息分组，度量字段求和，并在窗口闭合后补充窗口起始时间、结束时间以及当前统计时间。


        KeyedStream<TradeTrademarkCategoryUserRefundBean, Tuple3<String, String, String>> keyedDS = withWatermarkDS.keyBy(
                new KeySelector<TradeTrademarkCategoryUserRefundBean, Tuple3<String, String, String>>() {
                    @Override
                    public Tuple3<String, String, String> getKey(TradeTrademarkCategoryUserRefundBean bean) {
                        return Tuple3.of(
                                bean.getTrademarkId(),
                                bean.getCategory3Id(),
                                bean.getUserId()
                        );
                    }
                }
        );

        keyedDS.print("keyedDS:");

        WindowedStream<TradeTrademarkCategoryUserRefundBean, Tuple3<String, String, String>, TimeWindow> windowDS
                = keyedDS.window(TumblingEventTimeWindows.of(Time.seconds(10)));


        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> reduceDS = windowDS.reduce(
                new ReduceFunction<TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public TradeTrademarkCategoryUserRefundBean reduce(TradeTrademarkCategoryUserRefundBean value1, TradeTrademarkCategoryUserRefundBean value2) {
                        value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                        return value1;
                    }
                },
                new WindowFunction<TradeTrademarkCategoryUserRefundBean, TradeTrademarkCategoryUserRefundBean, Tuple3<String, String, String>, TimeWindow>() {
                    @Override
                    public void apply(Tuple3<String, String, String> stringStringStringTuple3, TimeWindow window, Iterable<TradeTrademarkCategoryUserRefundBean> input, Collector<TradeTrademarkCategoryUserRefundBean> out) {
                        TradeTrademarkCategoryUserRefundBean bean = input.iterator().next();
                        bean.setStt(DateFormatUtil.tsToDateTime(window.getStart()));
                        bean.setEdt(DateFormatUtil.tsToDateTime(window.getEnd()));
                        bean.setCurDate(DateFormatUtil.tsToDate(window.getStart()));
                        bean.setRefundCount((long) bean.getOrderIdSet().size());
                        System.out.println("bean:" + bean);
                        System.out.println("time:" + DateFormatUtil.tsToDateTime(bean.getTs()));
                        out.collect(bean);
                    }
                }
        );

        reduceDS.print("reduceDS:");

        //6）补充与分组无关的维度信息
        //（1）关联base_trademark表
        //获取tm_name。
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> tmNameDS = AsyncDataStream.unorderedWait(
                reduceDS,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public void addDims(TradeTrademarkCategoryUserRefundBean obj, JSONObject dimJsonObj) {
                        obj.setTrademarkName(dimJsonObj.getString("tm_name"));
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_trademark";
                    }

                    @Override
                    public String getRowKey(TradeTrademarkCategoryUserRefundBean obj) {
                        return obj.getTrademarkId();
                    }
                },
                120,
                TimeUnit.SECONDS
        );
        //（2）关联base_category3表
        //获取name（三级品类名称），获取category2_id。
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> category3NameDS = AsyncDataStream.unorderedWait(
                tmNameDS,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public void addDims(TradeTrademarkCategoryUserRefundBean obj, JSONObject dimJsonObj) {
                        obj.setCategory3Name(dimJsonObj.getString("name"));
                        obj.setCategory2Id(dimJsonObj.getString("category2_id"));
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_category3";
                    }

                    @Override
                    public String getRowKey(TradeTrademarkCategoryUserRefundBean obj) {
                        return obj.getCategory3Id();
                    }
                },
                120,
                TimeUnit.SECONDS
        );
        //（3）关联base_categroy2表
        //获取name（二级品类名称），category1_id。
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> category2NameDS = AsyncDataStream.unorderedWait(
                category3NameDS,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public void addDims(TradeTrademarkCategoryUserRefundBean obj, JSONObject dimJsonObj) {
                        obj.setCategory2Name(dimJsonObj.getString("name"));
                        obj.setCategory1Id(dimJsonObj.getString("category1_id"));
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_category2";
                    }

                    @Override
                    public String getRowKey(TradeTrademarkCategoryUserRefundBean obj) {
                        return obj.getCategory2Id();
                    }
                },
                120,
                TimeUnit.SECONDS
        );
        //（4）关联base_category1 表
        //获取name（一级品类名称）。
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> category1NameDS = AsyncDataStream.unorderedWait(
                category2NameDS,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public void addDims(TradeTrademarkCategoryUserRefundBean obj, JSONObject dimJsonObj) {
                        obj.setCategory1Name(dimJsonObj.getString("name"));
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_category1";
                    }

                    @Override
                    public String getRowKey(TradeTrademarkCategoryUserRefundBean obj) {
                        return obj.getCategory1Id();
                    }
                },
                120,
                TimeUnit.SECONDS
        );

        //7）写出到 Doris
        category1NameDS.print("category1NameDS");
        category1NameDS
                .map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_trade_trademark_category_user_refund_window"));

    }
}
