package com.lzhpo.pulsarflink;

import com.lzhpo.pulsarflink.source.PulsarFlinkSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * Flink Pulsar Test
 *
 * @author lzhpo
 */
public class SourceTest {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        PulsarFlinkSource<String> builder = PulsarFlinkSource.builder(
                new SimpleStringSchema())
                .pulsarServerUrl("pulsar://192.168.200.109:6650")
                .topic("topic1")
                .subscriptionName("test-sub");

        env.addSource(builder)
                .flatMap(new FlatMapFunction<byte[], String>() {
                    @Override
                    public void flatMap(byte[] value, Collector<String> out) throws Exception {
                        System.out.println("value: " + value);
                        String lines = new String(value);
                        System.out.println("lines: " +lines);
                        String[] split = lines.split(",");
                        System.out.println("split：" + Arrays.toString(split));
                        for (String line : split) {
                            System.out.println("line: " +line);
                            out.collect(line);
                        }
                    }
                })
                .print()
                .setParallelism(2);

        env.execute("Flink pulsar flink source");

    }

}
