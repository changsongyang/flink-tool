package com.lzhpo.flinkkafka.sink;

import com.lzhpo.flinkkafka.config.KafkaProducerConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;

/**
 * TODO：Kafka数据倾斜的情况、Kafka事务(https://www.infoq.cn/article/kafka-analysis-part-8)
 *
 * <p>https://blog.csdn.net/learn_tech/article/details/80923308
 *
 * @author lzhpo
 */
@Slf4j
public class FlinkKafkaProducer01<T> extends RichSinkFunction<String> {

  /** topic */
  private String topic;

  /** 需要发送的数据key */
  private String key;

  /** 需要发送的数据value */
  private String value;

  /** KafkaConsumerConfig */
  private KafkaProducerConfig kafkaProducerConfig;

  /** 序列化 */
  private DeserializationSchema<T> deserializationSchema;

  /** Kafka生产者 */
  protected KafkaProducer<String, String> producer;

  public FlinkKafkaProducer01(
      String topic,
      String value,
      KafkaProducerConfig kafkaProducerConfig,
      DeserializationSchema<T> deserializationSchema) {
    this.topic = topic;
    this.value = value;
    this.kafkaProducerConfig = kafkaProducerConfig;
    this.deserializationSchema = deserializationSchema;
  }

  public FlinkKafkaProducer01(
      String topic,
      String key,
      String value,
      KafkaProducerConfig kafkaProducerConfig,
      DeserializationSchema<T> deserializationSchema) {
    this.topic = topic;
    this.key = key;
    this.value = value;
    this.kafkaProducerConfig = kafkaProducerConfig;
    this.deserializationSchema = deserializationSchema;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    producer = kafkaProducerConfig.createFactory();
  }

  @Override
  public void close() throws Exception {
    super.close();
    if (producer != null) {
      producer.close();
    }
  }

  @Override
  public void invoke(String s, Context context) {
    ProducerRecord<String, String> msg;
    if (key == null || "".equals(key)) {
      msg = new ProducerRecord<>(topic, value);
    } else {
      msg = new ProducerRecord<>(topic, key, value);
    }
    producer.send(
        msg,
        new Callback() {
          @Override
          public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (exception == null) {
              log.info("Send [key={},value={}] to {} topic successfully.", key, value, topic);
            } else {
              log.error("Send [key={},value={}] to {} topic fail.", key, value, topic, exception);
            }
          }
        });
    // 列出topic的相关信息
    List<PartitionInfo> partitionInfos;
    partitionInfos = producer.partitionsFor(topic);
    partitionInfos.forEach(System.out::println);
  }
}
