package com.lzhpo.flinkredis;

import com.lzhpo.flinkredis.config.RedisConnectionConfig;
import com.lzhpo.flinkredis.source.FlinkRedisSource;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Zhaopo Liu
 */
public class FlinkRedisSourceTest {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStreamSource = env.addSource(
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
        );

        dataStreamSource
                .print()
                .setParallelism(2);

        env.execute("Flink Redis Source Sink To Kafka");
    }

}
