package com.lzhpo.flinkkafka;

import com.lzhpo.flinkkafka.config.KafkaConsumerConfig;
import com.lzhpo.flinkkafka.source.KafkaSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 自定义Flink的Source测试
 *
 * kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic flink-consumer01-topic --from-beginning
 * <p>
 * kafka-console-producer.sh --broker-list localhost:9092 --topic flink-consumer01-topic
 *
 * @author Zhaopo Liu
 * @date 2020/6/20 03:14
 */
public class KafkaSourceTest {

    public static void main(String[] args) throws Exception {
        // init env
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 添加数据源
        DataStreamSource<String> consumerStreamSource =
                env.addSource(
                        new KafkaSource<>(
                                new SimpleStringSchema(),
                                KafkaConsumerConfig.builder()
                                        .setBootstrapServers("localhost:9092")
                                        .setGroupId("flink-consumer01-group-test")
                                        .setEnableAutoCommit(false)
                                        .setKeyDeserializer("org.apache.kafka.common.serialization.StringDeserializer")
                                        .setValueDeserializer("org.apache.kafka.common.serialization.StringDeserializer")
                                        .build(),
                                // Java8使用Stream来创建传入Topic的Set集合
                                Stream.of("flink-consumer01-topic").collect(Collectors.toSet())));

        // transformation操作
        SingleOutputStreamOperator<String> flatMapStream = consumerStreamSource
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String value, Collector<String> collector) throws Exception {
                        System.out.println("value:" + value);
                        collector.collect(value);
                    }
                });

        flatMapStream
                .print()
                .setParallelism(2);

        // execute
        env.execute("Flink kafka consumer01 job");
    }

}
