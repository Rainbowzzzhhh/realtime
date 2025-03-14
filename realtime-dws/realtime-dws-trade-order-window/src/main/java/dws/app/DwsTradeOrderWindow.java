package dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
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
import rainbow.realtime.common.bean.TradeOrderBean;
import rainbow.realtime.common.constant.Constant;
import rainbow.realtime.common.function.BeanToJsonStrMapFunction;
import rainbow.realtime.common.util.DateFormatUtil;
import rainbow.realtime.common.util.FlinkSinkUtil;

/**
 * @author rainbow
 * @time 2025-03-14 17:35
 * @description ...
 */
public class DwsTradeOrderWindow extends BaseApp {

    public static void main(String[] args) {
        new DwsTradeOrderWindow().start(10028, 4, "dws_trade_order_window", Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }

    //{"id":"34841","order_id":"19611","user_id":"2755","sku_id":"1",
// "sku_name":"","province_id":"12","activity_id":null,"activity_rule_id":null,
// "coupon_id":null,"date_id":"2025-03-14","create_time":"2025-03-14 12:48:18",
// "sku_num":"2","split_original_amount":"13998.0000","split_activity_amount":"900.0",
// "split_coupon_amount":"0.0","split_total_amount":"13098.0","ts":1741927698}
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {

        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);

        //3）设置水位线
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = jsonObjDS.assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps());

        //4）按照用户 id 分组
        KeyedStream<JSONObject, String> userIdKeyedDS = withWatermarkDS.keyBy(jsonObj -> jsonObj.getString("user_id"));

        //5）计算度量字段的值
        SingleOutputStreamOperator<TradeOrderBean> beanDS = userIdKeyedDS.process(new KeyedProcessFunction<String, JSONObject, TradeOrderBean>() {

            private ValueState<String> lastOrderDateState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> lastOrderDateStateDescriptor = new ValueStateDescriptor<>("lastOrderDateState", String.class);
                lastOrderDateState = getRuntimeContext().getState(lastOrderDateStateDescriptor);
            }

            @Override
            public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, TradeOrderBean>.Context ctx, Collector<TradeOrderBean> out) throws Exception {
                String lastOrderDate = lastOrderDateState.value();
                Long ts = jsonObj.getLong("ts") * 1000;
                String curOrderDate = DateFormatUtil.tsToDate(ts);
                long orderUniqueUserCount = 0L;
                long orderNewUserCount = 0L;
                //运用Flink状态编程，在状态中维护用户末次下单日期。
                //若末次下单日期为null，则将首次下单用户数和下单独立用户数均置为1；
                //否则首次下单用户数置为 0，判断末次下单日期是否为当日，如果不是当日则下单独立用户数置为1,否则置为0。最后将状态中的下单日期更新为当日。

                if (StringUtils.isEmpty(lastOrderDate)) {
                    orderUniqueUserCount = 1L;
                    orderNewUserCount = 1L;
                    lastOrderDateState.update(curOrderDate);
                } else if (!lastOrderDate.equals(curOrderDate)) {
                    orderUniqueUserCount = 1L;
                    lastOrderDateState.update(curOrderDate);
                }
                if (orderUniqueUserCount == 1L) {
                    out.collect(new TradeOrderBean("", "", "", orderUniqueUserCount, orderNewUserCount, ts));
                }
            }
        });

        //6）开窗、聚合
        //度量字段求和，补充窗口起始时间和结束时间字段，ts 字段置为当前系统时间戳。
        AllWindowedStream<TradeOrderBean, TimeWindow> windowDS = beanDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)));

        SingleOutputStreamOperator<TradeOrderBean> reduceDS = windowDS.reduce(new ReduceFunction<TradeOrderBean>() {
            @Override
            public TradeOrderBean reduce(TradeOrderBean value1, TradeOrderBean value2) throws Exception {
                value1.setOrderUniqueUserCount(value1.getOrderUniqueUserCount() + value2.getOrderUniqueUserCount());
                value1.setOrderNewUserCount(value1.getOrderNewUserCount() + value2.getOrderNewUserCount());
                return value1;
            }
        }, new AllWindowFunction<TradeOrderBean, TradeOrderBean, TimeWindow>() {
            @Override
            public void apply(TimeWindow window, Iterable<TradeOrderBean> values, Collector<TradeOrderBean> out) throws Exception {
                TradeOrderBean tradeOrderBean = values.iterator().next();
                tradeOrderBean.setStt(DateFormatUtil.tsToDateTime(window.getStart()));
                tradeOrderBean.setEdt(DateFormatUtil.tsToDateTime(window.getEnd()));
                tradeOrderBean.setCurDate(DateFormatUtil.tsToDate(window.getEnd()));
                out.collect(tradeOrderBean);
            }
        });
        reduceDS.print();
        //7）写出到Doris。
        reduceDS.map(new BeanToJsonStrMapFunction<>()).sinkTo(FlinkSinkUtil.getDorisSink("dws_trade_order_window"));
    }
}






























