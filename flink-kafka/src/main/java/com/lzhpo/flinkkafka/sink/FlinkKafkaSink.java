package com.lzhpo.flinkkafka.sink;

import com.lzhpo.common.SendData;
import com.lzhpo.flinkkafka.KafkaCallback;
import com.lzhpo.flinkkafka.KafkaConfigConstant;
import com.lzhpo.flinkkafka.KafkaProducerFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Preconditions;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Flink Sink Kafka
 *
 * @author lzhpo
 */
@Slf4j
public class FlinkKafkaSink<T> extends RichSinkFunction<String> {

    /**
     * 序列化
     */
    DeserializationSchema<T> deserializationSchema;

    /**
     * Kafka生产者
     */
    Producer<String, String> producer;

    public FlinkKafkaSink(DeserializationSchema<T> deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
    }

    /**
     * 初始化连接Kafka
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Properties props = new KafkaProducerFactory().createFactory();
        producer = new KafkaProducer<>(props);
    }

    @Override
    public void close() throws Exception {
        super.close();
        producer.close();
    }

    /**
     * Producer由一个持有未发送消息记录的资源池和一个用来向Kafka集群发送消息记录的后台IO线程组成。
     * 使用后未关闭producer将导致这些资源泄露。
     *
     * @param value
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(String value, Context context) throws Exception {
        if (KafkaConfigConstant.SEND_DATA != null) {
            producer.send(new ProducerRecord<>(
                            KafkaConfigConstant.TOPIC,
                            KafkaConfigConstant.SEND_DATA.toString()),
                    // 异步发送消息，发送失败自定义处理
                    new KafkaCallback());
        } else {
            log.warn("Please set sendData!");
        }
        close();
    }

    /**
     * 构建一个PulsarSource
     *
     * @param deserializationSchema the deserializer used to convert between Pulsar's byte messages and Flink's objects.
     * @return a builder
     */
    public static <T> FlinkKafkaSink<T> builder(DeserializationSchema<T> deserializationSchema) {
        Preconditions.checkNotNull(deserializationSchema, "deserializationSchema cannot be null");
        return new FlinkKafkaSink<>(deserializationSchema);
    }

    public FlinkKafkaSink<T> setBootstrapServers(String bootstrapServers) {
        KafkaConfigConstant.BOOTSTRAP_SERVERS = bootstrapServers;
        return this;
    }

    public FlinkKafkaSink<T> setAcks(String acks) {
        KafkaConfigConstant.ACKS = acks;
        return this;
    }

    public FlinkKafkaSink<T> setRetries(int retries) {
        KafkaConfigConstant.RETRIES = retries;
        return this;
    }

    public FlinkKafkaSink<T> setBatchSize(int batchSize) {
        KafkaConfigConstant.BATCH_SIZE = batchSize;
        return this;
    }

    public FlinkKafkaSink<T> setLingerMs(int lingerMs) {
        KafkaConfigConstant.LINGER_MS = lingerMs;
        return this;
    }

    public FlinkKafkaSink<T> setBufferMemory(int bufferMemory) {
        KafkaConfigConstant.BUFFER_MEMORY = bufferMemory;
        return this;
    }

    public FlinkKafkaSink<T> setKeySerializer(String keySerializer) {
        KafkaConfigConstant.KEY_SERIALIZER = keySerializer;
        return this;
    }

    public FlinkKafkaSink<T> setValueSerializer(String valueSerializer) {
        KafkaConfigConstant.VALUE_SERIALIZER = valueSerializer;
        return this;
    }

    public FlinkKafkaSink<T> setTopic(String topic) {
        KafkaConfigConstant.TOPIC = topic;
        return this;
    }

    public FlinkKafkaSink<T> setSendData(SendData sendData) {
        KafkaConfigConstant.SEND_DATA = sendData;
        return this;
    }
}
