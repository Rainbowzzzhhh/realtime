package dws.app;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import rainbow.realtime.common.Base.BaseApp;
import rainbow.realtime.common.constant.Constant;

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


    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        //String 转换为 JSONObject。
        //3）设置水位线
        //4）开窗、聚合
        //5）写入 Doris

    }
}
