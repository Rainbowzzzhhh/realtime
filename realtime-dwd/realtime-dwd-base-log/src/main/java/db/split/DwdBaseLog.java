package db.split;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import rainbow.realtime.common.Base.BaseApp;
import rainbow.realtime.common.constant.Constant;

/**
 * @author rainbow
 * @time 2025-03-04 15:59
 * @description 日志分流
 */
public class DwdBaseLog extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwdBaseLog().start(10011,4,"dwd_base_log", Constant.TOPIC_LOG);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        //TODO 对流中数据类型进行转换 并做简单ETl
        //定义侧输出流标签
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
