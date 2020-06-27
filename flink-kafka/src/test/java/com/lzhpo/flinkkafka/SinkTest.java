package com.lzhpo.flinkkafka;

import com.lzhpo.common.SendData;
import com.lzhpo.flinkkafka.sink.FlinkKafkaSink;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Date;
import java.util.Random;

/**
 * 先在Kafka创建一个topic：
 * kafka-topics.sh --zookeeper localhost:2181 --create --replication-factor 1 --partitions 1 --topic topic1
 *
 * 然后创建一个消费者：
 * kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic topic1 --from-beginning
 *
 * @author lzhpo
 */
public class SinkTest {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStreamSource = env.fromElements("lzhpo");

        dataStreamSource.addSink(
                new FlinkKafkaSink<>(new SimpleStringSchema())
                        .setBootstrapServers("192.168.200.109:9092")
                        .setTopic("topic1")
                        .setSendData(
                                SendData.builder()
                                        .id(new Random().nextLong())
                                        .body("HelloWorld!")
                                        .createDate(new Date())
                                        .creator("lzhpo")
                                        .builded()
                        )
        ).setParallelism(2);

        env.execute("Flink sink to kafka");
    }

}
