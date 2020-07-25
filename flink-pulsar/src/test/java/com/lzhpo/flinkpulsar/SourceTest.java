package com.lzhpo.flinkpulsar;

import com.lzhpo.flinkpulsar.config.PulsarConsumerConfig;
import com.lzhpo.flinkpulsar.source.FlinkPulsarConsumer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.pulsar.client.api.SubscriptionType;

/**
 * Flink Pulsar Test
 *
 * @author Zhaopo Liu
 * @date 2020/6/20 03:14
 */
public class SourceTest {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStreamSource = env.addSource(
                new FlinkPulsarConsumer<>(
                        new SimpleStringSchema(),
                        PulsarConsumerConfig.builder()
                                .setPulsarUrl("pulsar://localhost:6650")
//                                .setPulsarToken("123")
                                .setTopic("default-test-topic")
                                .setAckTimeout(10)
                                .setSubscriptionName("default-test-sub")
                                .setSubscriptionType(SubscriptionType.Exclusive)
                                .build(),
                        true
                )
        );

        dataStreamSource
                .print()
                .setParallelism(2);

        env.execute("Flink pulsar flink source");
    }
}
