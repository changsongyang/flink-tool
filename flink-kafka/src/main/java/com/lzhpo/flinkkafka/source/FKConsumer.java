package com.lzhpo.flinkkafka.source;

import com.lzhpo.flinkkafka.config.KafkaConsumerConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.io.IOException;
import java.time.Duration;
import java.util.Set;

/**
 * Kafka Source
 *
 * @author Zhaopo Liu
 * @date 2020/6/20 03:14
 */
@Slf4j
public class FKConsumer<OUT> extends RichSourceFunction<OUT> implements ResultTypeQueryable<OUT> {

    /**
     * Kafka消费者
     */
    protected Consumer<String, String> consumer;

    /**
     * 反序列化
     */
    private DeserializationSchema<OUT> schema;

    /**
     * Kafka消费者的配置
     */
    private final KafkaConsumerConfig kafkaConsumerConfig;

    /**
     * 消费者订阅的topic
     */
    private final Set<String> topics;

    /**
     * 读取topic数据的超时时间
     */
    protected int pollTimeOut = 5000;

    /**
     * 控制任务运行/停止
     */
    protected boolean running = true;

    /**
     * 通过构造函数注入序列化配置、Kafka消费者的配置、订阅的topic列表
     *
     * @param deserializationSchema 序列化
     * @param kafkaConsumerConfig   Kafka消费者配置
     * @param topics                消费者订阅的topic列表
     */
    public FKConsumer(
            DeserializationSchema<OUT> deserializationSchema,
            KafkaConsumerConfig kafkaConsumerConfig,
            Set<String> topics) {
        this.schema = deserializationSchema;
        this.kafkaConsumerConfig = kafkaConsumerConfig;
        this.topics = topics;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 构建一个消费者
        consumer = kafkaConsumerConfig.createFactory();
        log.info("Create new consumer successfully!");
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (consumer != null) {
            // 关闭
            consumer.close();
            log.info("Already close kafka connection.");
        }
    }

    @Override
    public void run(SourceContext<OUT> ctx) {
        consumer.subscribe(topics);
        while (running) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMinutes(pollTimeOut));
            records.forEach(
                    record -> {
                        log.info("Receive new message: " + record.toString());
                        OUT result = null;
                        try {
                            result = schema.deserialize(record.toString().getBytes());
                        } catch (IOException e) {
                            log.error("record [{}] deserialize failed.", record.value(), e);
                        }
                        // Flink收集数据
                        ctx.collect(result);
                        // 异步提交offset
                        consumer.commitAsync(
                                (offsets, exception) -> {
                                    log.info("offsets:{}", offsets);
                                    if (exception == null) {
                                        log.info("Commit offset successfully!");
                                    } else {
                                        log.error("Commit offset fail!{}", exception.getMessage());
                                    }
                                });
                    });
        }
    }

    @Override
    public void cancel() {
        running = false;
        log.info("Flink job already cancel!");
    }

    /**
     * 获取反序列化的值的数据类型
     *
     * @return The data type produced by this function or input format.
     */
    @Override
    public TypeInformation<OUT> getProducedType() {
        return schema.getProducedType();
    }
}
