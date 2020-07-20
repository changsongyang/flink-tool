package com.lzhpo.flinkhive.sink;

import com.google.gson.Gson;
import com.lzhpo.common.modeltest.UserModelTest;
import com.lzhpo.flinkhive.config.HiveConnectionConfig;
import com.lzhpo.flinkkafka.config.KafkaConsumerConfig;
import com.lzhpo.flinkkafka.source.KafkaSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Zhaopo Liu
 * @date 2020/7/20 14:52
 */
public class TestFlinkKafkaSinkHive {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 添加数据源
        DataStreamSource<ConsumerRecord<String, String>> consumerStreamSource =
                env.addSource(
                        new KafkaSource<>(
                                new SimpleStringSchema(),
                                KafkaConsumerConfig.builder()
                                        .setBootstrapServers("192.168.200.109:9092")
                                        .setGroupId("flink-consumer01-test")
                                        .setEnableAutoCommit(false)
                                        .setKeyDeserializer("org.apache.kafka.common.serialization.StringDeserializer")
                                        .setValueDeserializer("org.apache.kafka.common.serialization.StringDeserializer")
                                        .build(),
                                // Java8使用Stream来创建传入Topic的Set集合
                                Stream.of("flink-mysql-sink-topic").collect(Collectors.toSet())));

        // transformation
        SingleOutputStreamOperator<UserModelTest> flatMapStream = consumerStreamSource.flatMap(new FlatMapFunction<ConsumerRecord<String, String>, UserModelTest>() {
            @Override
            public void flatMap(ConsumerRecord<String, String> value, Collector<UserModelTest> collector) throws Exception {
                UserModelTest user = new Gson().fromJson(value.value(), UserModelTest.class);
                collector.collect(user);
            }
        });

        // sink
        flatMapStream.addSink(
                new HiveSink<>(
                        new SimpleStringSchema(),
                        HiveConnectionConfig.builder()
                                .setDriverName("org.apache.hive.jdbc.HiveDriver")
                                .setUrl("jdbc:hive2://localhost:10000/db_comprs")
                                .setUsername("root")
                                .setPassword("123456")
                                .build()
                )
        );

        env.execute("Flink kafka sink hive job");
    }

}
