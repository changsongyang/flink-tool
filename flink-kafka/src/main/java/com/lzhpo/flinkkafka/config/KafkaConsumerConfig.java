package com.lzhpo.flinkkafka.config;

import com.lzhpo.common.BaseFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

/**
 * Kafka consumer config
 *
 * @author Zhaopo Liu
 * @date 2020/6/20 03:14
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
     *
     * @return Builder
     */
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final KafkaConsumerConfig kafkaConsumerConfig;

        public Builder() {
            kafkaConsumerConfig = new KafkaConsumerConfig();
        }

        public Builder setBootstrapServers(String bootstrapServers) {
            kafkaConsumerConfig.bootstrapServers = bootstrapServers;
            return this;
        }

        public Builder setGroupId(String groupId) {
            kafkaConsumerConfig.groupId = groupId;
            return this;
        }

        public Builder setEnableAutoCommit(boolean enableAutoCommit) {
            kafkaConsumerConfig.enableAutoCommit = enableAutoCommit;
            return this;
        }

        public Builder setKeyDeserializer(String keyDeserializer) {
            kafkaConsumerConfig.keyDeserializer = keyDeserializer;
            return this;
        }

        public Builder setValueDeserializer(String valueDeserializer) {
            kafkaConsumerConfig.valueDeserializer = valueDeserializer;
            return this;
        }

        public KafkaConsumerConfig build() {
            return kafkaConsumerConfig;
        }
    }
}
