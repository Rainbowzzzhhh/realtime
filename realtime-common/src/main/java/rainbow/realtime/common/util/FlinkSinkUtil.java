package rainbow.realtime.common.util;

import com.alibaba.fastjson.JSONObject;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.kafka.clients.producer.ProducerRecord;
import rainbow.realtime.common.bean.TableProcessDwd;
import rainbow.realtime.common.constant.Constant;

import javax.annotation.Nullable;
import java.util.Properties;

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
                        return new ProducerRecord<byte[], byte[]>(topic, jsonObject.toString().getBytes());
                    }
                })
                //.setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)   //决定是否开启事务，保证kafka的精准一次性
                //.setTransactionalIdPrefix("dwd_base_log_")  //设置事务id的前缀
                //.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 15 * 60 * 1000 + "")    //设置kafka事务超时时间，需要大于检查点时间并小于最大事务超时时间15min
                .build();
    }


    /**
     * 写入doris表的数据类型为jsonObj<br>
     * 即:dataStream<JsonObject>.getDorisSink(String table)
     * @param table
     * @return  DorisSink<String>
     */
    public static DorisSink<String> getDorisSink(String table) {
        Properties props = new Properties();
        props.setProperty("format", "json");
        props.setProperty("read_json_by_line", "true"); // 每行一条 json 数据
        return DorisSink.<String>builder()
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisOptions(DorisOptions.builder() // 设置 doris 的连接参数
                        .setFenodes(Constant.DORIS_FE_NODES)
                        .setTableIdentifier(Constant.DORIS_DATABASE + "." + table)
                        .setUsername("root")
                        .setPassword("000000")
                        .build()
                )
                .setDorisExecutionOptions(DorisExecutionOptions.builder() // 执行参数
                        //.setLabelPrefix(labelPrefix)  // stream-load 导入数据时 label 的前缀
                        .disable2PC() // 开启两阶段提交后,labelPrefix 需要全局唯一,为了测试方便禁用两阶段提交
                        .setBufferCount(3) // 批次条数: 默认 3
                        .setBufferSize(1024 * 1024) // 批次大小: 默认 1M
                        .setCheckInterval(3000) // 批次输出间隔  上述三个批次的限制条件是或的关系
                        .setMaxRetries(3)
                        .setStreamLoadProp(props) // 设置 stream load 的数据格式 默认是 csv,需要改成 json
                        .build())
                .setSerializer(new SimpleStringSerializer())
                .build();
    }
}

