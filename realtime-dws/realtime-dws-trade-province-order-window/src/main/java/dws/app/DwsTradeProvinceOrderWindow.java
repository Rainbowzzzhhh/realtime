package dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
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
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import rainbow.realtime.common.Base.BaseApp;
import rainbow.realtime.common.bean.TradeProvinceOrderBean;
import rainbow.realtime.common.constant.Constant;
import rainbow.realtime.common.function.BeanToJsonStrMapFunction;
import rainbow.realtime.common.function.DimAsyncFunction;
import rainbow.realtime.common.util.DateFormatUtil;
import rainbow.realtime.common.util.FlinkSinkUtil;

import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * @author rainbow
 * @time 2025-03-14 10:11
 * @description 订单明细数据，过滤null数据并按照唯一键对数据去重，统计各省份各窗口订单数和订单金额
 * {"id":"12668","order_id":"6538","user_id":"1892",
 * "sku_id":"31","sku_name":"","province_id":"19",
 * "activity_id":null,"activity_rule_id":null,
 * "coupon_id":"1","date_id":"2025-03-08",
 * "create_time":"2025-03-08 00:18:29","sku_num":"2",
 * "split_original_amount":"138.0000","split_activity_amount":"0.0",
 * "split_coupon_amount":"30.0","split_total_amount":"108.0","ts":1741401643}
 */

public class DwsTradeProvinceOrderWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTradeProvinceOrderWindow().start(10020, 4, "dws_trade_province_order_window", Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {

        //TODO 1.过滤空消息（通常flink自动过滤），并对流中的数据类型转换
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                if (jsonStr != null) {
                    out.collect(JSON.parseObject(jsonStr));
                }
            }
        });

        //TODO 2.按照唯一键（订单明细id）进行分组
        KeyedStream<JSONObject, String> IdKeyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getString("id"));

        //TODO 3.去重
        SingleOutputStreamOperator<JSONObject> distinctDS = IdKeyedDS.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
            private ValueState<JSONObject> lastJsonObjState;

            @Override
            public void open(Configuration parameters) {
                ValueStateDescriptor<JSONObject> lastJsonObjStateDescriptor = new ValueStateDescriptor<>("lastJsonObjState", JSONObject.class);
                lastJsonObjStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10)).build());
                lastJsonObjState = getRuntimeContext().getState(lastJsonObjStateDescriptor);
            }

            @Override
            public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                JSONObject lastJsonObj = lastJsonObjState.value();
                if (lastJsonObj != null) {
                    lastJsonObj.put("split_total_amount", "-" + lastJsonObj.getString("split_total_amount"));
                    out.collect(lastJsonObj);
                }
                lastJsonObjState.update(jsonObj);
                out.collect(jsonObj);
            }
        });

        //distinctDS.print("distinctDS:");
        //TODO 4.指定watermark以及提取事件时间字段
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = distinctDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forMonotonousTimestamps().withTimestampAssigner((SerializableTimestampAssigner<JSONObject>) (element, recordTimestamp) -> element.getLong("ts") * 1000));

        //TODO 5.再次对流中数据进行类型转换 jsonObj->统计的实体类对象
        SingleOutputStreamOperator<TradeProvinceOrderBean> beanDS = withWatermarkDS.map((MapFunction<JSONObject, TradeProvinceOrderBean>) jsonObj -> TradeProvinceOrderBean.builder().provinceId(jsonObj.getString("province_id")).orderAmount(jsonObj.getBigDecimal("split_total_amount")).orderIdSet(new HashSet<>(Collections.singleton(jsonObj.getString("order_id")))).ts(jsonObj.getLong("ts")).build());

        //beanDS.print("beanDS:");
        //TODO 6.分组
        KeyedStream<TradeProvinceOrderBean, String> keyedDS = beanDS.keyBy(TradeProvinceOrderBean::getProvinceId);

        //keyedDS.print("keyedDS");
        //TODO 7.开窗     window对各组独立进行开窗，windowAll对整个流进行开窗
        WindowedStream<TradeProvinceOrderBean, String, TimeWindow> windowDS = keyedDS.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));

        //TODO 8.聚合
        SingleOutputStreamOperator<TradeProvinceOrderBean> reduceDS = windowDS.reduce(
                new ReduceFunction<TradeProvinceOrderBean>() {
                    @Override
                    public TradeProvinceOrderBean reduce(TradeProvinceOrderBean value1, TradeProvinceOrderBean value2) {
                        value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                        value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                        return value1;
                    }
                },
                new WindowFunction<TradeProvinceOrderBean, TradeProvinceOrderBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<TradeProvinceOrderBean> input, Collector<TradeProvinceOrderBean> out) throws Exception {
                        TradeProvinceOrderBean orderBean = input.iterator().next();
                        orderBean.setStt(DateFormatUtil.tsToDateTime(window.getStart()));
                        orderBean.setEdt(DateFormatUtil.tsToDateTime(window.getEnd()));
                        orderBean.setCurDate(DateFormatUtil.tsToDate(window.getStart()));
                        orderBean.setOrderCount((long) orderBean.getOrderIdSet().size());
                        out.collect(orderBean);
                    }
                }
        );

        //reduceDS.print("reduceDS:");
        //TODO 9.关联province维度
        SingleOutputStreamOperator<TradeProvinceOrderBean> withProvinceDS = AsyncDataStream.unorderedWait(
                reduceDS,
                new DimAsyncFunction<TradeProvinceOrderBean>() {
                    @Override
                    public void addDims(TradeProvinceOrderBean obj, JSONObject dimJsonObj) {
                        obj.setProvinceName(dimJsonObj.getString("name"));
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_province";
                    }

                    @Override
                    public String getRowKey(TradeProvinceOrderBean obj) {
                        return obj.getProvinceId();
                    }
                },
                120,
                TimeUnit.SECONDS
        );
        withProvinceDS.print("withProvinceDS:");

        //TODO 15.将结果写到doris
        withProvinceDS
                .map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_trade_province_order_window"));
    }
}



































