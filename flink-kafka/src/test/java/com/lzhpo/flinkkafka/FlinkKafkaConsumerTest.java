package com.lzhpo.flinkkafka;

import com.lzhpo.flinkkafka.config.KafkaConsumerConfig;
import com.lzhpo.flinkkafka.source.FlinkKafkaConsumer01;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * kafka-console-consumer.sh --bootstrap-server 192.168.200.109:9092 --topic flink-consumer01-topic
 * --from-beginning
 *
 * <p>kafka-console-producer.sh --broker-list 192.168.200.109:9092 --topic flink-consumer01-topic
 *
 * @author lzhpo
 */
public class FlinkKafkaConsumerTest {

  public static void main(String[] args) throws Exception {
    // init env
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // 添加数据源
    DataStreamSource<ConsumerRecord<String, String>> consumerStreamSource =
        env.addSource(
            new FlinkKafkaConsumer01<>(
                new SimpleStringSchema(),
                KafkaConsumerConfig.builder()
                    .setBootstrapServers("192.168.200.109:9092")
                    .setGroupId("flink-consumer01-test")
                    .setEnableAutoCommit(false)
                    .setKeyDeserializer("org.apache.kafka.common.serialization.StringDeserializer")
                    .setValueDeserializer(
                        "org.apache.kafka.common.serialization.StringDeserializer")
                    .build(),
                // Java8使用Stream来创建传入Topic的Set集合
                Stream.of("flink-consumer01-topic").collect(Collectors.toSet())));

    // transformation操作
    consumerStreamSource
//            .keyBy("timestamp")
            .print()
            .setParallelism(2);

    // execute
    env.execute("Flink kafka consumer01 job");
  }
}