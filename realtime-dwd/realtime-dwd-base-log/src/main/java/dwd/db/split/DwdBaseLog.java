package dwd.db.split;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import rainbow.realtime.common.Base.BaseApp;
import rainbow.realtime.common.constant.Constant;
import rainbow.realtime.common.util.DateFormatUtil;
import rainbow.realtime.common.util.FlinkSinkUtil;

/**
 * @author rainbow
 * @time 2025-03-04 15:59
 * @description 日志分流
 * 需要启动的进程：zk,kafka,flume
 * <p>
 * kafkaSource:
 * 从kafka读取数据
 * 通过手动维护偏移量，保证消费的精确一次
 * kafkaSink:
 * 写入数据到kafka
 * 精准一次需开启检查点
 * .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
 * .setTransactionalIdPrefix("dwd_base_log_")  //设置事务id的前缀
 * .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 15 * 60 * 1000 + "")
 * 在消费端，需要设置消费隔离级别为读已提交 kafkaSource的setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
 */

public class DwdBaseLog extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwdBaseLog().start(10011, 4, "dwd_base_log", Constant.TOPIC_LOG);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        //TODO 对流中数据类型进行转换 并做简单ETl
        SingleOutputStreamOperator<JSONObject> jsonObjDS = etl(kafkaStrDS);

        //TODO 对新老访客标记进行修复
        SingleOutputStreamOperator<JSONObject> fixedDS = fixedNewAndOld(jsonObjDS);
        //fixedDS.print();
        //TODO 分流 错误放到错误测输出流 启动 曝光 动作 ，页面放到主流
        //定义侧输出流标签
        OutputTag<String> errTag = new OutputTag<>("errTag", TypeInformation.of(String.class));

        OutputTag<String> startTag = new OutputTag<>("startTag", TypeInformation.of(String.class));

        OutputTag<String> displayTag = new OutputTag<>("displayTag", TypeInformation.of(String.class));

        OutputTag<String> actionTag = new OutputTag<>("actionTag", TypeInformation.of(String.class));

        //分流
        SingleOutputStreamOperator<String> pageDS = fixedDS.process(
                new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject jsonObj, ProcessFunction<JSONObject, String>.Context context, Collector<String> collector) {

                        //错误日志
                        if (jsonObj.containsKey("err")) {
                            //输出到错误侧输出流
                            context.output(errTag, jsonObj.toJSONString());
                            jsonObj.remove("err");
                        }

                        //启动日志
                        if (jsonObj.containsKey("start")) {
                            context.output(startTag, jsonObj.toJSONString());
                        } else {
                            JSONObject common = jsonObj.getJSONObject("common");
                            JSONObject page = jsonObj.getJSONObject("page");
                            Long ts = jsonObj.getLong("ts");

                            //曝光日志
                            JSONArray displays = jsonObj.getJSONArray("displays");

                            if (displays != null && !displays.isEmpty()) {
                                for (int i = 0; i < displays.size(); i++) {
                                    JSONObject display = displays.getJSONObject(i);
                                    JSONObject newDisplayJsonObj = new JSONObject();

                                    newDisplayJsonObj.put("common", common);
                                    newDisplayJsonObj.put("page", page);
                                    newDisplayJsonObj.put("display", display);
                                    newDisplayJsonObj.put("ts", ts);

                                    context.output(displayTag, newDisplayJsonObj.toJSONString());
                                }
                                jsonObj.remove("displays");
                            }

                            //动作日志
                            JSONArray actions = jsonObj.getJSONArray("actions");

                            if (actions != null && !actions.isEmpty()) {
                                for (int i = 0; i < actions.size(); i++) {
                                    JSONObject action = actions.getJSONObject(i);
                                    JSONObject newActionJsonObj = new JSONObject();

                                    newActionJsonObj.put("common", common);
                                    newActionJsonObj.put("page", page);
                                    newActionJsonObj.put("action", action); //含有ts

                                    context.output(actionTag, newActionJsonObj.toJSONString());
                                }
                                jsonObj.remove("actions");
                            }

                            //页面日志 写到主流
                            collector.collect(jsonObj.toJSONString());
                        }
                    }
                }
        );

        SideOutputDataStream<String> errDS = pageDS.getSideOutput(errTag);
        SideOutputDataStream<String> startDS = pageDS.getSideOutput(startTag);
        SideOutputDataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        SideOutputDataStream<String> actionDS = pageDS.getSideOutput(actionTag);
        pageDS.print("页面日志:");
        errDS.print("错误日志:");
        startDS.print("启动日志:");
        displayDS.print("曝光日志:");
        actionDS.print("动作日志:");

        //TODO 将不同流的数据写到kafka不同主题中
        pageDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));
        errDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));
        startDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));
        displayDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));
        actionDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));
    }

    private static SingleOutputStreamOperator<JSONObject> fixedNewAndOld(SingleOutputStreamOperator<JSONObject> jsonObjDS) {
        //按照设备id分组
        KeyedStream<JSONObject, String> keyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));
        //修复，使用flink状态编程
        return keyedDS.map(
                new RichMapFunction<JSONObject, JSONObject>() {

                    private ValueState<String> lastVisitDateState;

                    @Override
                    public void open(Configuration parameters) {
                        ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("lastVisitDateState", String.class);
                        lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public JSONObject map(JSONObject value) throws Exception {
                        //获取is_new的值
                        String isNew = value.getJSONObject("common").getString("is_new");
                        //从状态获取首次访问日期
                        String lastVisitDate = lastVisitDateState.value();
                        //获取当前访问时间
                        Long ts = value.getLong("ts");
                        String curVisitDate = DateFormatUtil.tsToDate(ts);

                        //① 如果is_new的值为1
                        if ("1".equals(isNew)) {
                            if (StringUtils.isEmpty(lastVisitDate)) {
                                //	如果键控状态为null，认为本次是该访客首次访问 APP，将日志中 ts 对应的日期更新到状态中，不对 is_new 字段做修改；
                                lastVisitDateState.update(curVisitDate);
                            } else {
                                //	如果键控状态不为null，且首次访问日期不是当日，说明访问的是老访客，将 is_new 字段置为 0；
                                if (!lastVisitDate.equals(curVisitDate))
                                    value.getJSONObject("common").put("is_nuw", "0");

                                //	如果键控状态不为 null，且首次访问日期是当日，说明访问的是新访客，不做操作；
                            }

                        } else {
                            //② 如果 is_new 的值为 0
                            //	如果键控状态为 null，说明访问 APP 的是老访客但本次是该访客的页面日志首次进入程序。当前端新老访客状态标记丢失时，日志进入程序被判定为新访客，Flink 程序就可以纠正被误判的访客状态标记，只要将状态中的日期设置为今天之前即可。本程序选择将状态更新为昨日；
                            if (StringUtils.isEmpty(lastVisitDate)) {
                                String yesterday = DateFormatUtil.tsToDate(ts - 24 * 60 * 60 * 1000);
                                lastVisitDateState.update(yesterday);
                            }
                        }

                        return value;
                    }
                }
        );
    }

    private static SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> kafkaStrDS) {
        //定义侧输出流标签
        OutputTag<String> dirtyTag = new OutputTag<>("dirtyTag", TypeInformation.of(String.class));

        //ETL
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) {
                try {
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    //未发生异常为标准json，传递数据到下游
                    collector.collect(jsonObj);
                } catch (Exception e) {
                    //发生异常，不为标准json，为脏数据，放到侧输出流中
                    context.output(dirtyTag, jsonStr);
                }
            }
        });

        //jsonObjDS.print("标准json");
        SideOutputDataStream<String> dirtyDS = jsonObjDS.getSideOutput(dirtyTag);
        //dirtyDS.print("脏数据");

        //将侧输出流中的脏数据写到kafka主题中
        KafkaSink<String> kafkaSink = FlinkSinkUtil.getKafkaSink("dirty_data");
        dirtyDS.sinkTo(kafkaSink);
        return jsonObjDS;
    }
}


































