package com.lzhpo.flinkkafka.source;

import com.lzhpo.flinkkafka.config.KafkaConsumerConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;
import java.util.Set;

/**
 * Kafka消费者01
 *
 * @author lzhpo
 */
@Slf4j
public class FlinkKafkaConsumer01<T> extends RichSourceFunction<ConsumerRecord<String, String>> {

  /** Kafka消费者 */
  protected Consumer<String, String> consumer;

  /** 序列化 */
  private DeserializationSchema<T> deserializationSchema;

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
  public FlinkKafkaConsumer01(
      DeserializationSchema<T> deserializationSchema,
      KafkaConsumerConfig kafkaConsumerConfig,
      Set<String> topics) {
    this.deserializationSchema = deserializationSchema;
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
  public void run(SourceContext<ConsumerRecord<String, String>> ctx) {
    consumer.subscribe(topics);
    while (running) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMinutes(this.pollTimeOut));
      records.forEach(
          record -> {
            log.info("Receive new message: " + record.toString());
            // Flink收集数据
            ctx.collect(record);
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
}
