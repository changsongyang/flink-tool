package com.lzhpo.flinkkafka;

import com.lzhpo.flinkkafka.sink.FlinkKafkaSink;
import com.lzhpo.flinkkafka.source.FlinkKafkaSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.ArrayList;

/**
 * kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic1 topic1 --from-beginning
 * <p>
 * kafka-console-producer.sh --broker-list localhost:9092 --topic topic1
 *
 * @author lzhpo
 */
public class ConsumerTest {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ArrayList<String> subTopics = new ArrayList<>();
        subTopics.add("topic1");

        DataStreamSource<ConsumerRecord<String, String>> dataStreamSource = env.addSource(
                FlinkKafkaSource.builder(new SimpleStringSchema())
                        .setBootstrapServers("192.168.200.109:9092")
                        .setGroupId("testGroupId")
                        .setSubscribeTopics(subTopics)
                        .setEnableAutoCommit(true));

        dataStreamSource
                .print()
                .setParallelism(2);

        env.execute("Flink kafka source");
    }

}
