package com.lzhpo.flinkredis;

import com.lzhpo.flinkkafka.config.KafkaConsumerConfig;
import com.lzhpo.flinkkafka.source.FlinkKafkaConsumer01;
import com.lzhpo.flinkredis.config.RedisConnectionConfig;
import com.lzhpo.flinkredis.sink.FlinkRedisSink;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.HashMap;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Flink read kafka data sink to redis
 *
 * <p>Kafka Shell: kafka-console-producer.sh --broker-list 192.168.200.109:9092 --topic
 * flink-consumer01-topic
 *
 * @author lzhpo
 */
public class FlinkRedisSinkTest {

  public static void main(String[] args) throws Exception {
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

    // 过滤出key-value形式的
    SingleOutputStreamOperator<HashMap<String, String>> flatMapStream =
        consumerStreamSource.flatMap(
            new FlatMapFunction<ConsumerRecord<String, String>, HashMap<String, String>>() {
              @Override
              public void flatMap(
                  ConsumerRecord<String, String> value,
                  Collector<HashMap<String, String>> collector)
                  throws Exception {
                HashMap<String, String> map = new HashMap<>();
                // 暂且先UUID作为测试...
                map.put(UUID.randomUUID().toString(), value.value());
                collector.collect(map);
              }
            });

    // sink to redis
    flatMapStream
        .addSink(
            new FlinkRedisSink<>(
                new SimpleStringSchema(),
                RedisConnectionConfig.builder()
                    .setRedisUrl("192.168.200.109")
                    .setRedisPort(6379)
                    .setPassWord("123456")
                    .setDataBase(0)
                    .build(),
                60))
        .setParallelism(2)
        .name("Flink Redis Sink");

    // execute
    env.execute("Flink Redis Sink Job");
  }
}
