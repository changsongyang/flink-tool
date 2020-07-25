package com.lzhpo.flinkpulsar;

import com.lzhpo.flinkpulsar.config.PulsarProducerConfig;
import com.lzhpo.flinkpulsar.sink.FlinkPulsarProducer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Flink Pulsar Sink Test
 *
 * @author Zhaopo Liu
 * @date 2020/6/20 03:14
 */
public class SinkTest {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // env.fromElements将一系列元素作为数据源
        DataStreamSource<String> dataStreamSource = env.fromCollection(
                Stream.of("666666666", "9999999", "HelloWorld!").collect(Collectors.toList()));

        dataStreamSource.addSink(
                new FlinkPulsarProducer<>(
                        new SimpleStringSchema(),
                        PulsarProducerConfig.builder()
                                .setPulsarUrl("pulsar://localhost:6650")
//                                .setPulsarToken("123")
                                .setTopic("default-test-topic")
                                .setBatchingMaxPublishDelay(10)
                                .setSendTimeout(10)
                                .setBlockIfQueueFull(true)
                                .build(),
                        true
                )
        );

        env.execute("Flink pulsar flink sink");
    }
}
