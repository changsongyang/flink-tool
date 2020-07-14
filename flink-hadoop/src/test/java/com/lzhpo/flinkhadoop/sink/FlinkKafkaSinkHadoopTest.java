package com.lzhpo.flinkhadoop.sink;

import com.lzhpo.flinkhadoop.config.HadoopConnectionConfig;
import com.lzhpo.flinkkafka.config.KafkaConsumerConfig;
import com.lzhpo.flinkkafka.source.FlinkKafkaConsumer01;
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
 * 读取Kafka中的指定topic数据sink到Hadoop中
 *
 * @author lzhpo
 */
public class FlinkKafkaSinkHadoopTest {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 这里的值随便一个东西就行，主要是为了要使用DataStreamSource来使用sink
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
                                        .setValueDeserializer("org.apache.kafka.common.serialization.StringDeserializer")
                                        .build(),
                                // Java8使用Stream来创建传入Topic的Set集合
                                Stream.of("flink-mysql-sink-topic").collect(Collectors.toSet())));
        // flatMapStream
        SingleOutputStreamOperator<byte[]> flatMapStream = consumerStreamSource.flatMap(new FlatMapFunction<ConsumerRecord<String, String>, byte[]>() {
            @Override
            public void flatMap(ConsumerRecord<String, String> value, Collector<byte[]> collector) throws Exception {
                byte[] bytes = value.value().getBytes();
                collector.collect(bytes);
            }
        });

        // sink to hdfs
        flatMapStream.addSink(
                new FlinkKafkaSinkHadoop<>(
                        new SimpleStringSchema(),
                        HadoopConnectionConfig.builder()
                                .setHadoopUrl("localhost:9000")
                                .setUser("root")
                                .build(),
                        "/flink/flink-hadoop/file/a.txt",
                        true,
                        4096
                )
        );

        // execute
        env.execute("Flink Kafka Sink Hadoop Test");
    }

}
