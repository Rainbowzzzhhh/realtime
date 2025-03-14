package dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import rainbow.realtime.common.Base.BaseApp;
import rainbow.realtime.common.bean.TradePaymentBean;
import rainbow.realtime.common.constant.Constant;
import rainbow.realtime.common.function.BeanToJsonStrMapFunction;
import rainbow.realtime.common.util.DateFormatUtil;
import rainbow.realtime.common.util.FlinkSinkUtil;

/**
 * @author rainbow
 * @time 2025-03-14 11:51
 * @description ...
 */
public class DwsTradePaymentSucWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTradePaymentSucWindow().start(
                10027,
                4,
                "dws_trade_payment_suc_window",
                Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS
        );
    }

    //{"order_detail_id":"13780","order_id":"7096","user_id":"1864",
    // "sku_id":"8","sku_name":"","province_id":"9","activity_id":null,
    // "activity_rule_id":null,"coupon_id":null,"payment_type_code":"1101",
    // "payment_type_name":"支付宝","callback_time":"2025-03-08 00:17:31","sku_num":"1",
    // "split_original_amount":"8197.0000","split_activity_amount":"0.0","split_coupon_amount":"0.0",
    // "split_payment_amount":"8197.0","ts":1741405967}
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {

        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);

        SingleOutputStreamOperator<JSONObject> withWatermarkDS = jsonObjDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                        return element.getLong("ts") * 1000;
                                    }
                                }
                        )
        );

        KeyedStream<JSONObject, String> userIdKeyedDS = withWatermarkDS.keyBy(jsonObj -> jsonObj.getString("user_id"));
        //userIdKeyedDS.print("userIdKeyedDS");

        SingleOutputStreamOperator<TradePaymentBean> payUuNewDS = userIdKeyedDS.process(
                new KeyedProcessFunction<String, JSONObject, TradePaymentBean>() {
                    private ValueState<String> lastPayDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {

                        ValueStateDescriptor<String> lastPayDateStateDescriptor
                                = new ValueStateDescriptor<>("lastPayDateState", String.class);
                        lastPayDateState = getRuntimeContext().getState(lastPayDateStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, TradePaymentBean>.Context ctx, Collector<TradePaymentBean> out) throws Exception {
                        String lastPayDate = lastPayDateState.value();
                        long ts = jsonObj.getLong("ts") * 1000;
                        String curPayDate = DateFormatUtil.tsToDate(ts);
                        long payUuCount = 0L;
                        long payNewCount = 0L;

                        if (lastPayDate == null) {
                            payUuCount = 1L;
                            payNewCount = 1L;
                            lastPayDateState.update(curPayDate);
                        } else if (!lastPayDate.equals(curPayDate)) {
                            lastPayDateState.update(curPayDate);
                            payUuCount = 1L;
                        }
                        if (payUuCount == 1L) {
                            out.collect(new TradePaymentBean("", "", "", payUuCount, payNewCount, ts));
                        }
                    }
                }
        );
        //payUuNewDS.print("payUuNewDS:");

        AllWindowedStream<TradePaymentBean, TimeWindow> windowDS = payUuNewDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)));

        SingleOutputStreamOperator<TradePaymentBean> reduceDS = windowDS.reduce(
                new ReduceFunction<TradePaymentBean>() {
                    @Override
                    public TradePaymentBean reduce(TradePaymentBean value1, TradePaymentBean value2) {
                        value1.setPaymentSucUniqueUserCount(value1.getPaymentSucUniqueUserCount() + value2.getPaymentSucUniqueUserCount());
                        value1.setPaymentSucNewUserCount(value1.getPaymentSucNewUserCount() + value2.getPaymentSucNewUserCount());
                        return value1;
                    }
                },
                new AllWindowFunction<TradePaymentBean, TradePaymentBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<TradePaymentBean> values, Collector<TradePaymentBean> out) throws Exception {
                        TradePaymentBean payBean = values.iterator().next();
                        payBean.setStt(DateFormatUtil.tsToDateTime(window.getStart()));
                        payBean.setEdt(DateFormatUtil.tsToDateTime(window.getEnd()));
                        payBean.setCurDate(DateFormatUtil.tsToDate(window.getStart()));
                        out.collect(payBean);
                    }
                }
        );

        reduceDS.print("reduceDS");
        reduceDS
                .map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_trade_payment_suc_window"));

    }
}






















