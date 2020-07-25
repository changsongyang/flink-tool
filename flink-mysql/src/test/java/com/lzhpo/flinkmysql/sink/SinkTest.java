package com.lzhpo.flinkmysql.sink;

import com.google.gson.Gson;
import com.lzhpo.common.modeltest.UserModelTest;
import com.lzhpo.flinkkafka.config.KafkaConsumerConfig;
import com.lzhpo.flinkkafka.source.FkConsumer;
import com.lzhpo.flinkmysql.config.MysqlConnectionConfig;
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
 * 读取Kafka中指定的topic数据sink到MySQL
 *
 * @author Zhaopo Liu
 * @date 2020/6/20 03:14
 */
public class SinkTest {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 添加数据源
        DataStreamSource<ConsumerRecord<String, String>> consumerStreamSource =
                env.addSource(
                        new FkConsumer<>(
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
                new FlinkKafkaSinkMysql<>(
                        new SimpleStringSchema(),
                        MysqlConnectionConfig.builder()
                                .setUrl("jdbc:mysql://localhost:3306/study-flink?useSSL=false&useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=CTT&characterEncoding=UTF-8&autoReconnect=true&failOverReadOnly=false")
                                .setUsername("root")
                                .setPassword("123456")
                                .build()
                )
        );

        env.execute("Flink sink mysql job");
    }
}
