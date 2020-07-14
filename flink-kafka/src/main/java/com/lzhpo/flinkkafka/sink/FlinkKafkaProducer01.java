package com.lzhpo.flinkkafka.sink;

import com.lzhpo.flinkkafka.config.KafkaProducerConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * TODO：Kafka数据倾斜的情况、Kafka事务(https://www.infoq.cn/article/kafka-analysis-part-8)
 *
 * <p>https://blog.csdn.net/learn_tech/article/details/80923308
 *
 * @author lzhpo
 */
@Slf4j
public class FlinkKafkaProducer01<IN> extends RichSinkFunction<HashMap<String, String>> {

    /**
     * 序列化
     */
    private DeserializationSchema<IN> deserializationSchema;

    /**
     * topic
     */
    private String topic;

    /**
     * KafkaConsumerConfig
     */
    private KafkaProducerConfig kafkaProducerConfig;

    /**
     * Kafka生产者
     */
    protected KafkaProducer<String, String> producer;

    public FlinkKafkaProducer01(DeserializationSchema<IN> deserializationSchema, String topic, KafkaProducerConfig kafkaProducerConfig) {
        this.deserializationSchema = deserializationSchema;
        this.topic = topic;
        this.kafkaProducerConfig = kafkaProducerConfig;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        log.info("parameters:{}", parameters);
        super.open(parameters);
        producer = kafkaProducerConfig.createFactory();
        // 列出topic的相关信息
        List<PartitionInfo> partitionInfos;
        partitionInfos = producer.partitionsFor(topic);
        partitionInfos.forEach(System.out::println);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (producer != null) {
            producer.close();
        }
    }

    @Override
    public void invoke(HashMap<String, String> hashMap, Context context) {
        log.info("hashMap:{}", hashMap);
        ProducerRecord<String, String> msg;
        for (Map.Entry<String, String> entry : hashMap.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (key == null || "".equals(key)) {
                msg = new ProducerRecord<>(topic, value);
            } else {
                msg = new ProducerRecord<>(topic, key, value);
            }
            producer.send(
                    msg,
                    (metadata, exception) -> {
                        if (exception == null) {
                            log.info("Send [key={},value={}] to {} topic successfully.",
                                    key, value, topic);
                        } else {
                            log.error("Send [key={},value={}] to {} topic fail.",
                                    key, value, topic, exception);
                        }
                    }
            );
        }
    }

}
