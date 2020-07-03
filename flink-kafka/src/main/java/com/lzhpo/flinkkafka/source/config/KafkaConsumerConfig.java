package com.lzhpo.flinkkafka.source.config;

import com.lzhpo.common.BaseFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

/**
 * Kafka consumer config
 *
 * @author lzhpo
 */
public class KafkaConsumerConfig extends BaseFactory<Consumer<String, String>> {

    private String bootstrapServers = "localhost:9092";
    private String groupId = "default-group";
    private boolean enableAutoCommit = false;
    private String keyDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";
    private String valueDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";

    @Override
    public Consumer<String, String> createFactory() {
        Properties props = new Properties();
        props.put("bootstrap.servers", this.bootstrapServers);
        props.put("group.id", this.groupId);
        props.put("enable.auto.commit", this.enableAutoCommit);
        props.put("key.deserializer", this.keyDeserializer);
        props.put("value.deserializer", this.valueDeserializer);
        return new KafkaConsumer<>(props);
    }

    /**
     * This for {@link KafkaConsumerConfig} build
     * @return Builder
     */
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final KafkaConsumerConfig kafkaConnectionConfig;

        public Builder() {
            kafkaConnectionConfig = new KafkaConsumerConfig();
        }

        public Builder setBootstrapServers(String bootstrapServers) {
            kafkaConnectionConfig.bootstrapServers = bootstrapServers;
            return this;
        }

        public Builder setGroupId(String groupId) {
            kafkaConnectionConfig.groupId = groupId;
            return this;
        }

        public Builder setEnableAutoCommit(boolean enableAutoCommit) {
            kafkaConnectionConfig.enableAutoCommit = enableAutoCommit;
            return this;
        }

        public Builder setKeyDeserializer(String keyDeserializer) {
            kafkaConnectionConfig.keyDeserializer = keyDeserializer;
            return this;
        }

        public Builder setValueDeserializer(String valueDeserializer) {
            kafkaConnectionConfig.valueDeserializer = valueDeserializer;
            return this;
        }

        public KafkaConsumerConfig build() {
            return kafkaConnectionConfig;
        }
    }
}