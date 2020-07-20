package com.lzhpo.flinkkafka.sink;

import com.lzhpo.flinkkafka.config.KafkaProducerConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;

/**
 * TODO：Kafka数据倾斜的情况、Kafka事务(https://www.infoq.cn/article/kafka-analysis-part-8)
 *
 * <p>https://blog.csdn.net/learn_tech/article/details/80923308
 *
 * @author Zhaopo Liu
 * @date 2020/6/20 03:14
 */
@Slf4j
public class KafkaSink<IN> extends RichSinkFunction<IN> {

    /** 序列化 */
    private SerializationSchema<IN> schema;

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

    public KafkaSink(SerializationSchema<IN> serializationSchema,
                     String topic, KafkaProducerConfig kafkaProducerConfig) {
        this.schema = serializationSchema;
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
    public void invoke(IN value, Context context) {
        producer.send(
                new ProducerRecord<>(topic, new String(schema.serialize(value))),
                (metadata, exception) -> {
                    if (exception == null) {
                        log.info("send data [{}] to rabbitmq successfully.", value);
                    } else {
                        log.error("send data [{}] to rabbitmq failed.", value, exception);
                    }
                }
        );
    }

}
