package dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import rainbow.realtime.common.Base.BaseApp;
import rainbow.realtime.common.bean.CartAddUuBean;
import rainbow.realtime.common.constant.Constant;
import rainbow.realtime.common.function.BeanToJsonStrMapFunction;
import rainbow.realtime.common.util.DateFormatUtil;
import rainbow.realtime.common.util.FlinkSinkUtil;

/**
 * @author rainbow
 * @time 2025-03-12 22:14
 * @description ...
 */

public class DwsTradeCartAddUuWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTradeCartAddUuWindow().start(
                10026,
                4,
                "dws_trade_cart_add_uu_window",
                Constant.TOPIC_DWD_TRADE_CART_ADD
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        //2）将流中数据由 String 转换为 JSONObject。
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);
        //3）设置水位线
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = jsonObjDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                        return element.getLong("ts") * 1000;
                                    }
                                }
                        )
        );
        //4）按照用户 id 分组
        KeyedStream<JSONObject, String> keyedDS = withWatermarkDS.keyBy(jsonObj -> jsonObj.getString("user_id"));
        //5）过滤独立用户加购记录  运用 Flink 状态编程，将用户末次加购日期维护到状态中。  如果末次加购日期为 null 或者不等于当天日期，则保留数据并更新状态，否则丢弃，不做操作。
        SingleOutputStreamOperator<JSONObject> cartUUDS = keyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    private ValueState<String> lastCartDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("lastCartDateState", String.class);
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                        lastCartDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        String lastCartDate = lastCartDateState.value();
                        long ts = jsonObj.getLong("ts") * 1000;
                        String curCartDate = DateFormatUtil.tsToDate(ts);
                        if (StringUtils.isEmpty(lastCartDate)) {
                            out.collect(jsonObj);
                            lastCartDateState.update(curCartDate);
                        }

                    }
                }
        );
        //6）开窗、聚合   统计窗口中数据条数即为加购独立用户数，补充窗口起始时间、关闭时间，统计日期，发送到下游。
        AllWindowedStream<JSONObject, TimeWindow> windowDS
                = cartUUDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));
        SingleOutputStreamOperator<CartAddUuBean> aggregateDS = windowDS.aggregate(
                new AggregateFunction<JSONObject, Long, Long>() {
                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    @Override
                    public Long add(JSONObject value, Long accumulator) {
                        return ++accumulator;
                    }

                    @Override
                    public Long getResult(Long accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Long merge(Long a, Long b) {
                        return null;
                    }
                },
                new AllWindowFunction<Long, CartAddUuBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<Long> values, Collector<CartAddUuBean> out) throws Exception {
                        Long cartUUCt = values.iterator().next();
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDate = DateFormatUtil.tsToDate(window.getStart());
                        out.collect(new CartAddUuBean(stt, edt, curDate, cartUUCt));
                    }
                }
        );

        //7）将数据写入 Doris。
        aggregateDS.print();
        aggregateDS.map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_trade_cart_add_uu_window"));
    }
}


































