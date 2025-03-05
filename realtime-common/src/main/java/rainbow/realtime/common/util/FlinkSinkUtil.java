package rainbow.realtime.common.util;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import rainbow.realtime.common.constant.Constant;

/**
 * @author rainbow
 * @time 2025-03-05 17:07
 * @description 获取sink工具类
 */
public class FlinkSinkUtil {
    //获取kafkaSink
    public static KafkaSink<String> getKafkaSink(String topic){
        return KafkaSink.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                //.setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)   //决定是否开启事务，保证kafka的精准一次性
                //.setTransactionalIdPrefix("dwd_base_log_")  //设置事务id的前缀
                //.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 15 * 60 * 1000 + "")    //设置kafka事务超时时间，需要大于检查点时间并小于最大事务超时时间15min
                .build();
    }
}
