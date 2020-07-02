package com.lzhpo.flinkkafka.source;

import com.lzhpo.flinkkafka.source.config.KafkaConsumerConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.kafka.clients.consumer.*;

import java.time.Duration;
import java.util.Set;

/**
 * Kafka消费者01
 *
 * @author lzhpo
 */
@Slf4j
public class FlinkKafkaConsumer01<IN> extends RichSourceFunction<ConsumerRecord<String, String>> {

    /** Kafka消费者 */
    protected Consumer<String, String> consumer;

    /** 序列化 */
    private DeserializationSchema<IN> deserializationSchema;

    /** Kafka消费者的配置 */
    private final KafkaConsumerConfig kafkaConsumerConfig;

    /** 消费者订阅的topic */
    private final Set<String> topics;

    /** 读取topic数据的超时时间 */
    protected int pollTimeOut = 5000;

    /** 控制任务运行/停止 */
    protected boolean running = true;

    /**
     * 通过构造函数注入序列化配置、Kafka消费者的配置、订阅的topic列表
     *
     * @param deserializationSchema 序列化
     * @param kafkaConsumerConfig Kafka消费者配置
     * @param topics 消费者订阅的topic列表
     */
    public FlinkKafkaConsumer01(DeserializationSchema<IN> deserializationSchema, KafkaConsumerConfig kafkaConsumerConfig, Set<String> topics) {
        this.deserializationSchema = deserializationSchema;
        this.kafkaConsumerConfig = kafkaConsumerConfig;
        this.topics = topics;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 构建一个消费者
        consumer = kafkaConsumerConfig.createFactory();
        log.info("成功创建一个消费者!");
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (consumer != null) {
            // 关闭
            consumer.close();
            log.info("已关闭Kafka消费者连接！");
        }
    }

    @Override
    public void run(SourceContext<ConsumerRecord<String, String>> ctx) throws Exception {
        consumer.subscribe(topics);
        while (running) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMinutes(this.pollTimeOut));
            records.forEach(record -> {
                log.info("收到新消息: " + record.toString());
                // Flink收集数据
                ctx.collect(record);
            });
        }
    }

    @Override
    public void cancel() {
        running = false;
        log.info("Flink job already cancel！");
    }

}
