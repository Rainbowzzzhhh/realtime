package rainbow.realtime.common.util;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import rainbow.realtime.common.constant.Constant;

import java.io.IOException;

/**
 * @author rainbow
 * @time 2025-02-12 9:31
 * @description ...
 */
public class FlinkSourceUtil {
    //获取kafkaSource
    public static KafkaSource<String> getKafkaSource(String topic, String groupId) {
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
            .setBootstrapServers(Constant.KAFKA_BROKERS)
            .setTopics(topic)
            .setGroupId(groupId)
            .setStartingOffsets(OffsetsInitializer.latest())// 从末尾点开始消费    生产环境中为了保证精准一次性，需要手动维护偏移量
            //.setValueOnlyDeserializer(new SimpleStringSchema())// 使用flink提供的SimpleStringSchema时注意消息不能为空
            .setValueOnlyDeserializer(new DeserializationSchema<String>() {
                @Override
                public String deserialize(byte[] message) throws IOException {
                    if (message != null)
                        return new String(message);
                    return null;
                }

                @Override
                public boolean isEndOfStream(String nextElement) {
                    return false;
                }

                @Override
                public TypeInformation<String> getProducedType() {
                    return TypeInformation.of(String.class);
                }
            })
            .build();
        return kafkaSource;
    }
}
