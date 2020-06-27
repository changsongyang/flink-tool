package com.lzhpo.flinkkafka.source;

import com.lzhpo.flinkkafka.KafkaConfigConstant;
import com.lzhpo.flinkkafka.KafkaConsumerFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Preconditions;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author lzhpo
 */
@Slf4j
public class FlinkKafkaSource<T> extends RichSourceFunction<ConsumerRecord<String, String>> {

    /**
     * Kafka生产者
     */
    Consumer<String, String> consumer;

    /**
     * 序列化
     */
    DeserializationSchema<T> deserializationSchema;

    public FlinkKafkaSource(DeserializationSchema<T> deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Properties props = new KafkaConsumerFactory().createFactory();
        consumer = new KafkaConsumer<>(props);
    }

    @Override
    public void close() throws Exception {
        super.close();
        consumer.close();
    }

    @Override
    public void run(SourceContext<ConsumerRecord<String, String>> ctx) throws Exception {
        consumer.subscribe(KafkaConfigConstant.SUBSCRIBE_TOPICS);
        do {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMinutes(KafkaConfigConstant.POLL_TIMEOUT));
            // 如果是自动提交offset
            if (KafkaConfigConstant.ENABLE_AUTO_COMMIT) {
                for (ConsumerRecord<String, String> record : records) {
                    log.info("Received new message.The offset = [{}], key = [{}], value = [{}]", record.offset(), record.key(), record.value());
                    ctx.collect(record);
                }
            } else {
                // TODO：如果不是自动提交offset，使用观察者模式，如果消费量就...
                // 定义了一个ConsumerRecord的列表作为缓冲，当缓冲中的数据大于200条时，才一次性插入数据库中，并手动提交offset。
                final int minBatchSize = 200;
                List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
                if (buffer.size() >= minBatchSize) {
                    insertIntoDb(buffer);
                    consumer.commitAsync();
                    buffer.clear();
                }
            }
        } while (true);
    }

    // TODO：做数据库插入操作
    private void insertIntoDb(List<ConsumerRecord<String, String>> buffer) {

    }

    @Override
    public void cancel() {

    }


    /**
     * @param deserializationSchema the deserializer used to convert between Pulsar's byte messages and Flink's objects.
     * @return a builder
     */
    public static <T> FlinkKafkaSource<T> builder(DeserializationSchema<T> deserializationSchema) {
        Preconditions.checkNotNull(deserializationSchema, "deserializationSchema cannot be null");
        return new FlinkKafkaSource<>(deserializationSchema);
    }

    public FlinkKafkaSource<T> setBootstrapServers(String bootstrapServers) {
        KafkaConfigConstant.BOOTSTRAP_SERVERS = bootstrapServers;
        return this;
    }

    public FlinkKafkaSource<T> setGroupId(String groupId) {
        KafkaConfigConstant.GROUP_ID = groupId;
        return this;
    }

    public FlinkKafkaSource<T> setEnableAutoCommit(boolean enableAutoCommit) {
        KafkaConfigConstant.ENABLE_AUTO_COMMIT = enableAutoCommit;
        return this;
    }

    public FlinkKafkaSource<T> setAutoCommitIntervalMs(int autoCommitIntervalMs) {
        KafkaConfigConstant.AUTO_COMMIT_INTERVAL_MS = autoCommitIntervalMs;
        return this;
    }

    public FlinkKafkaSource<T> setKeyDeserializer(String keyDeserializer) {
        KafkaConfigConstant.KEY_DESERIALIZER = keyDeserializer;
        return this;
    }

    public FlinkKafkaSource<T> setValueDeserializer(String valueDeserializer) {
        KafkaConfigConstant.VALUE_DESERIALIZER = valueDeserializer;
        return this;
    }

    public FlinkKafkaSource<T> setSubscribeTopics(ArrayList<String> subscribeTopics) {
        KafkaConfigConstant.SUBSCRIBE_TOPICS = subscribeTopics;
        return this;
    }
}
