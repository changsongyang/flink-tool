package com.lzhpo.flinkredis;

import com.lzhpo.flinkredis.config.RedisConnectionConfig;
import com.lzhpo.flinkredis.sink.FlinkRedisSink;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkContextUtil;

import java.util.HashMap;
import java.util.Properties;

/**
 * @author lzhpo
 */
public class SinkTest {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStreamSource = env.fromElements("lzhpo");

        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.200.109:9092");
        props.put("zookeeper.connect", "192.168.200.109:2181");
        props.put("group.id", "metric-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");

        // DataStreamSource就是DataStream
//        DataStreamSource<String> dataStreamSource = env.addSource(new FlinkKafkaConsumer<>(
//                "transaction",
//                new SimpleStringSchema(),
//                props)
//                .setStartFromLatest())
//                // 默认是1
//                .setParallelism(2);
//
//        RedisConnectionConfig redisConnectionConfig =
//                RedisConnectionConfig.builder()
//                        .setRedisUrl("192.168.200.109")
//                        .setRedisPort(6379)
//                        .setDataBase(0)
//                        .setPassWord("123456")
//                        .build();
//
//        FlinkRedisSink flinkRedisSink = new FlinkRedisSink(redisConnectionConfig);
//        flinkRedisSink.invoke(map, SinkContextUtil.forTimestamp(0));
//
//        dataStreamSource.addSink(new FlinkRedisSink())

        env.execute("Flink Redis Sink Job");

    }

}
