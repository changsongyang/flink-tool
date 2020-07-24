package com.lzhpo.flinkkafka;

import com.lzhpo.flinkkafka.config.KafkaProducerConfig;
import com.lzhpo.flinkkafka.sink.KafkaSink;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 自定义Flink的Sink测试
 *
 * kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic kafka_sink_test --from-beginning
 * <p>
 * kafka-console-producer.sh --broker-list localhost:9092 --topic kafka_sink_test
 *
 * @author Zhaopo Liu
 * @date 2020/7/24 17:12
 */
public class KafkaSinkTest {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // source
        DataStreamSource<String> dataStreamSource = env.fromElements("HelloWorld");

        // sink
        dataStreamSource.addSink(
                new KafkaSink<>(
                        new SimpleStringSchema(),
                        "kafka_sink_test",
                        KafkaProducerConfig.builder()
                                .setBootstrapServers("localhost:9092")
                                .setAcks("all")
                                .setRetries(0)
                                .setBatchSize(16384)
                                .setLingerMs(1)
                                .setBufferMemory(33554432)
                                .setKeySerializer("org.apache.kafka.common.serialization.StringSerializer")
                                .setValueSerializer("org.apache.kafka.common.serialization.StringSerializer")
                                .build()
                )
        );

        env.execute("Kafka Sink Test");
    }

}
