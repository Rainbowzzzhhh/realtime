package rainbow.realtime.common.Base;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import rainbow.realtime.common.constant.Constant;
import rainbow.realtime.common.util.FlinkSourceUtil;

/**
 * @author rainbow
 * @time 2025-03-04 14:08
 * @description FlinkAPI的基类
 * 模板方法设计模式：
 * 在父类中定义完成某一个功能的核心算法骨架，具体地实现可以延迟到子类中完成。
 * 模板方法类一定是抽象类，里面有一套具体地实现流程（可以是抽象方法也可以是普通方法）。这些方法可能由上层模板继承而来。
 * 优点：在不改变父类核心算法骨架的前提下，每一个子类都可以有不同的实现。我们只需要关注具体方法的实现逻辑而不必在实现流程上分心。
 */

public abstract class BaseApp {
    /**
     * @param port         测试环境下启动本地WebUI的端口，为了避免本地端口冲突，做出以下规定：
     *                     （1）DIM层维度分流应用使用10001端口
     *                     （2）DWD层应用程序按照在本文档中出现的先后顺序，端口从10011开始，自增1
     *                     （3）DWS层应用程序按照在本文档中出现的先后顺序，端口从10021开始，自增1
     * @param parallelism  并行度，本项目统一设置为4。
     * @param ckAndGroupId 消费Kafka主题时的消费者组ID和检查点路径的最后一级目录名称，二者取值相同，为Job主程序类名的下划线命名形式。如DimApp的该参数取值为dim_app。
     * @param topic        消费的Kafka主题名称
     */
    public void start(int port, int parallelism, String ckAndGroupId, String topic) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", port);

        // TODO 1.获取执行环境
        // 1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // 1.2 设置并行度
        env.setParallelism(parallelism);

        // TODO 2.检查点相关设置

        // 2.1 开启检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);

        // 2.2 设置检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000L);

        // 2.3 设置job取消后检查点是否保留
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 2.4 设置两个检查点的最小间隔时间
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);

        // 2.5 设置重启策略
        // env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000L));
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30), Time.seconds(3)));

        // 2.6 设置状态后端以及检查点的存储路径
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/realtime/ck/" + ckAndGroupId);

        // 2.7 设置操作hadoop用户
        System.setProperty("HADOOP_USER_NAME", "root");

        // TODO 3.从Kafka的topic_db读取业务数据，封装为流
        // 3.1 声明消费主题以及消费者组
        // 3.2 创建消费者对象
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource(topic, ckAndGroupId);

        // 3.3 消费数据 封装为流
        DataStreamSource<String> kafkaStrDS
                = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // TODO 4.处理逻辑
        handle(env, kafkaStrDS);    //模板方法设计模式

        // TODO 5.提交作业
        env.execute();
    }

    public abstract void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS);
}
