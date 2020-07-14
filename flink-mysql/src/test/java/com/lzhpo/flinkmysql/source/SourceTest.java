package com.lzhpo.flinkmysql.source;

import com.lzhpo.flinkkafka.config.KafkaProducerConfig;
import com.lzhpo.flinkkafka.sink.FlinkKafkaProducer01;
import com.lzhpo.flinkmysql.config.MysqlConnectionConfig;
import com.lzhpo.flinkmysql.test.User;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.UUID;

/**
 * 读取MySQL中的数据sink到Kafka
 *
 * @author lzhpo
 */
public class SourceTest {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<User> mysqlStream = env.addSource(
                new FlinkKafkaSourceMysql<>(
                        new SimpleStringSchema(),
                        MysqlConnectionConfig.builder()
                                .setUrl("jdbc:mysql://localhost:3306/study-flink?useSSL=false&useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=CTT&characterEncoding=UTF-8&autoReconnect=true&failOverReadOnly=false")
                                .setUsername("root")
                                .setPassword("123456")
                                .build()
                )
        );

        SingleOutputStreamOperator<HashMap<String, String>> flatMapStream = mysqlStream.flatMap(new FlatMapFunction<User, HashMap<String, String>>() {
            @Override
            public void flatMap(User user, Collector<HashMap<String, String>> collector) throws Exception {
                HashMap<String, String> map = new HashMap<>();
                map.put(UUID.randomUUID().toString(), user.toString());
                collector.collect(map);
            }
        });

        flatMapStream.addSink(
                new FlinkKafkaProducer01<>(
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

        env.execute("Flink mysql source");
    }
}
