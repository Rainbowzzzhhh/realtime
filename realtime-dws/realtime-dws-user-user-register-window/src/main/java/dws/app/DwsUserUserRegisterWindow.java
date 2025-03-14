package dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import rainbow.realtime.common.Base.BaseApp;
import rainbow.realtime.common.bean.UserRegisterBean;
import rainbow.realtime.common.constant.Constant;
import rainbow.realtime.common.function.BeanToJsonStrMapFunction;
import rainbow.realtime.common.util.DateFormatUtil;
import rainbow.realtime.common.util.FlinkSinkUtil;

/**
 * @author rainbow
 * @time 2025-03-12 22:11
 * @description ...
 */
public class DwsUserUserRegisterWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsUserUserRegisterWindow().start(
                10025,
                4,
                "dws_user_user_register_window",
                Constant.TOPIC_DWD_USER_REGISTER
        );
    }

    //{"create_time":"2025-03-09 00:00:00","id":3285,"ts":1741456755}
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        //String 转换为 JSONObject。
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);
        //3）设置水位线
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


        //4）开窗、聚合
        AllWindowedStream<JSONObject, TimeWindow> windowDS = withWatermarkDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)));

        SingleOutputStreamOperator<UserRegisterBean> aggregateDS = windowDS.aggregate(
                new AggregateFunction<JSONObject, Long, Long>() {
                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    @Override
                    public Long add(JSONObject value, Long accumulator) {
                        return accumulator + 1L;
                    }

                    @Override
                    public Long getResult(Long accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Long merge(Long a, Long b) {
                        return a + b;
                    }
                },
                new AllWindowFunction<Long, UserRegisterBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<Long> values, Collector<UserRegisterBean> out) throws Exception {
                        Long registerNum = values.iterator().next();
                        out.collect(
                                new UserRegisterBean(
                                        DateFormatUtil.tsToDateTime(window.getStart()),
                                        DateFormatUtil.tsToDateTime(window.getEnd()),
                                        DateFormatUtil.tsToDate(window.getStart()),
                                        registerNum
                                )
                        );

                    }
                }
        );
        aggregateDS.print("aggregateDS:");
        //5）写入 Doris

        aggregateDS
                .map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_user_user_register_window"));

    }
}
