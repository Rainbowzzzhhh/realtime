package db.split;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import rainbow.realtime.common.Base.BaseApp;
import rainbow.realtime.common.constant.Constant;

/**
 * @author rainbow
 * @time 2025-03-04 15:59
 * @description 日志分流
 * 需要启动的进程：zk,kafka,flume
 */

public class DwdBaseLog extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwdBaseLog().start(10011, 4, "dwd_base_log", Constant.TOPIC_LOG);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        //TODO 对流中数据类型进行转换 并做简单ETl
        //定义侧输出流标签
        OutputTag<String> dirtyTag = new OutputTag<>("dirtyTag");

        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        try {
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            //未发生异常为标准json，传递数据到下游
                            collector.collect(jsonObj);
                        } catch (Exception e) {
                            //发生异常，不为标准json，为脏数据，放到侧输出流中
                            context.output(dirtyTag, jsonStr);
                        }
                    }
                }
        );
        jsonObjDS.print("标准json");
        SideOutputDataStream<String> dirtyDS = jsonObjDS.getSideOutput(dirtyTag);
        dirtyDS.print("脏数据");
        //ETL
        //将侧输出流中的脏数据写到kafka主题中
        //TODO 对新老访客标记进行修复
        //按照设备id分组
        //修复，使用flink状态编程
        //TODO 分流 错误放到错误测输出流 启动 曝光 动作 ，页面放到主流
        //定义侧输出流标签
        //分流
        //TODO 将不同流的数据写到kafka不同主题中
    }
}
