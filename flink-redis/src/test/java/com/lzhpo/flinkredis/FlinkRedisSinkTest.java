package com.lzhpo.flinkredis;

import com.google.gson.Gson;
import com.lzhpo.flinkredis.config.RedisConnectionConfig;
import com.lzhpo.flinkredis.sink.RedisSink;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.HashMap;

/**
 * Flink read kafka data sink to redis
 *
 * <p>Kafka Shell: kafka-console-producer.sh --broker-list 192.168.200.109:9092 --topic
 * flink-consumer01-topic
 *
 * @author Zhaopo Liu
 * @date 2020/6/20 03:14
 */
public class FlinkRedisSinkTest {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 添加数据源
        DataStreamSource<String> dataStreamSource = env.fromElements("HelloWorld!");

        // 过滤出key-value形式的，HashMap => JSON
        SingleOutputStreamOperator<String> flatMapStream = dataStreamSource.flatMap(
                new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String value, Collector<String> collector) throws Exception {
                        HashMap<String, String> map = new HashMap<>();
                        // 暂且先UUID作为测试...
                        map.put("lzhpo", value);
                        collector.collect(new Gson().toJson(map));
                    }
                }
        );

        // sink to redis
        flatMapStream
                .addSink(
                        new RedisSink<>(
                                new SimpleStringSchema(),
                                RedisConnectionConfig.builder()
                                        .setRedisUrl("192.168.200.109")
                                        .setRedisPort(6379)
                                        .setPassWord("123456")
                                        .setDataBase(0)
                                        .build(),
                                60))
                .setParallelism(2)
                .name("Flink Redis Sink");

        // execute
        env.execute("Flink Redis Sink Job");
    }
}
