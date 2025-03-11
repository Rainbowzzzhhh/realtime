package rainbow.realtime.common.util;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.kafka.clients.producer.ProducerRecord;
import rainbow.realtime.common.bean.TableProcessDwd;
import rainbow.realtime.common.constant.Constant;

import javax.annotation.Nullable;

/**
 * @author rainbow
 * @time 2025-03-05 17:07
 * @description 获取sink工具类
 */
public class FlinkSinkUtil {
    //获取kafkaSink
    public static KafkaSink<String> getKafkaSink(String topic) {
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

    public static KafkaSink<Tuple2<JSONObject, TableProcessDwd>> getKafkaSink() {
        return KafkaSink.<Tuple2<JSONObject, TableProcessDwd>>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(new KafkaRecordSerializationSchema<Tuple2<JSONObject, TableProcessDwd>>() {
                    @Nullable
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(Tuple2<JSONObject, TableProcessDwd> tup2, KafkaSinkContext context, Long timestamp) {
                        JSONObject jsonObject = tup2.f0;
                        TableProcessDwd tableProcessDwd = tup2.f1;
                        String topic = tableProcessDwd.getSinkTable();
                        return new ProducerRecord<byte[], byte[]>(topic,jsonObject.toString().getBytes());
                    }
                })
                //.setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)   //决定是否开启事务，保证kafka的精准一次性
                //.setTransactionalIdPrefix("dwd_base_log_")  //设置事务id的前缀
                //.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 15 * 60 * 1000 + "")    //设置kafka事务超时时间，需要大于检查点时间并小于最大事务超时时间15min
                .build();
    }
}
