package com.lzhpo.flinkredis;

import com.lzhpo.flinkkafka.config.KafkaProducerConfig;
import com.lzhpo.flinkkafka.sink.FKProducer;
import com.lzhpo.flinkredis.config.RedisConnectionConfig;
import com.lzhpo.flinkredis.source.FlinkRedisSource;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;

/**
 * @author Zhaopo Liu
 */
public class FlinkRedisSourceTest {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // add source
        SingleOutputStreamOperator<HashMap<String, String>> redisSourceStream = env.addSource(
                new FlinkRedisSource<>(
                        new SimpleStringSchema(),
                        RedisConnectionConfig.builder()
                                .setRedisUrl("192.168.200.109")
                                .setRedisPort(6379)
                                .setPassWord("123456")
                                .setDataBase(0)
                                .build(),
                        "lzhpo"
                )
        ).name("Flink Redis Add Source");

        // sink
        redisSourceStream.addSink(
                new FKProducer<>(
                        new SimpleStringSchema(),
                        "flink-producer01-test",
                        KafkaProducerConfig.builder()
                                .setBootstrapServers("192.168.200.109:9092")
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

        env.execute("Flink Redis Source Sink To Kafka");
    }

}
