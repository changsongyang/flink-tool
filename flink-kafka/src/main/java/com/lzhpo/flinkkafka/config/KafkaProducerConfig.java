package com.lzhpo.flinkkafka.config;

import com.lzhpo.common.BaseFactory;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

/**
 * @author lzhpo
 */
public class KafkaProducerConfig extends BaseFactory<KafkaProducer<String, String>> {

    /**
     * bootstrapServers
     */
    private String bootstrapServers = "localhost:9092";

    /**
     * acks：生产者需要server端在接收到消息后，进行反馈确认的尺度，主要用于消息的可靠性传输 。
     *
     * <p>1.acks=0表示生产者不需要来自server的确认。
     *
     * <p>2.acks=1表示server端将消息保存后即可发送ack，而不必等到其他follower角色的都收到了该消息。
     *
     * <p>3.acks=all(or acks=-1)意味着server端将等待所有的副本都被接收后才发送确认。
     */
    private String acks = "all";

    /**
     * retries：生产者发送失败后，重试的次数
     */
    private int retries = 0;

    /**
     * batch.size：当多条消息发送到同一个partition时，该值控制生产者批量发送消息的大小，批量发送可以减少生产者到服务端的请求数，有助于提高客户端和服务端的性能。
     */
    private int batchSize = 16384;

    /**
     * linger.ms：batch.size和linger.ms是两种实现让客户端每次请求尽可能多的发送消息的机制，它们可以并存使用，并不冲突。
     */
    private int lingerMs = 1;

    /**
     * buffer.memory：生产者缓冲区的大小，保存的是还未来得及发送到server端的消息，如果生产者的发送速度大于消息被提交到server端的速度，该缓冲区将被耗尽。
     */
    private int bufferMemory = 33554432;

    /**
     * keySerializer：key.serializer,value.serializer说明了使用何种序列化方式将用户提供的key和vaule值序列化成字节。
     */
    private String keySerializer = "org.apache.kafka.common.serialization.StringSerializer";

    /**
     * valueSerializer：key.serializer,value.serializer说明了使用何种序列化方式将用户提供的key和vaule值序列化成字节。
     */
    private String valueSerializer = "org.apache.kafka.common.serialization.StringSerializer";

    @Override
    public KafkaProducer<String, String> createFactory() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("acks", acks);
        props.put("retries", retries);
        props.put("batch.size", batchSize);
        props.put("linger.ms", lingerMs);
        props.put("buffer.memory", bufferMemory);
        props.put("key.serializer", keySerializer);
        props.put("value.serializer", valueSerializer);
        return new KafkaProducer<>(props);
    }

    /**
     * This for {@link KafkaProducerConfig} build
     *
     * @return Builder
     */
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final KafkaProducerConfig kafkaProducerConfig;

        public Builder() {
            kafkaProducerConfig = new KafkaProducerConfig();
        }

        public Builder setBootstrapServers(String bootstrapServers) {
            kafkaProducerConfig.bootstrapServers = bootstrapServers;
            return this;
        }

        public Builder setAcks(String acks) {
            kafkaProducerConfig.acks = acks;
            return this;
        }

        public Builder setRetries(int retries) {
            kafkaProducerConfig.retries = retries;
            return this;
        }

        public Builder setBatchSize(int batchSize) {
            kafkaProducerConfig.batchSize = batchSize;
            return this;
        }

        public Builder setLingerMs(int lingerMs) {
            kafkaProducerConfig.lingerMs = lingerMs;
            return this;
        }

        public Builder setBufferMemory(int bufferMemory) {
            kafkaProducerConfig.bufferMemory = bufferMemory;
            return this;
        }

        public Builder setKeySerializer(String keySerializer) {
            kafkaProducerConfig.keySerializer = keySerializer;
            return this;
        }

        public Builder setValueSerializer(String valueSerializer) {
            kafkaProducerConfig.valueSerializer = valueSerializer;
            return this;
        }

        public KafkaProducerConfig build() {
            return kafkaProducerConfig;
        }
    }
}
