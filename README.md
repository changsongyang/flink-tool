## Documentation

I have encapsulated the unified format, and the other operations are similar, so I won't continue to describe it. I will continue to expand more functions around Flink in the future.

### Example for kafka
#### sink
```java
public class KafkaSinkTest {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStreamSource = env.fromElements("HelloWorld");

        dataStreamSource.addSink(
                new FKProducer<>(
                        new SimpleStringSchema(),
                        UUID.randomUUID().toString(),
                        "kafka_sink_test",
                        KafkaProducerConfig.builder()
                                .setBootstrapServers("localhost:9092")
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

        env.execute("Kafka Sink Test");
    }

}
```
#### source
```java
public class KafkaSourceTest {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> consumerStreamSource =
                env.addSource(
                        new FKConsumer<>(
                                new SimpleStringSchema(),
                                KafkaConsumerConfig.builder()
                                        .setBootstrapServers("localhost:9092")
                                        .setGroupId("flink-consumer01-group-test")
                                        .setEnableAutoCommit(false)
                                        .setKeyDeserializer("org.apache.kafka.common.serialization.StringDeserializer")
                                        .setValueDeserializer("org.apache.kafka.common.serialization.StringDeserializer")
                                        .build(),
                                Stream.of("flink-consumer01-topic").collect(Collectors.toSet())));

        SingleOutputStreamOperator<String> flatMapStream = consumerStreamSource
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String value, Collector<String> collector) throws Exception {
                        // TODO some transformation
                        System.out.println("value:" + value);
                        collector.collect(value);
                    }
                });

        flatMapStream
                .print()
                .setParallelism(2);

        // execute
        env.execute("Flink kafka consumer01 job");
    }

}
```
### Example for pulsar
#### sink
```java
public class SinkTest {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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
```
You can use the kafka connector of this tool as a data source:

```java
		DataStreamSource<String> consumerStreamSource =
                env.addSource(
                        new FKConsumer<>(
                                new SimpleStringSchema(),
                                KafkaConsumerConfig.builder()
                                        .setBootstrapServers("localhost:9092")
                                        .setGroupId("flink-consumer01-group-test")
                                        .setEnableAutoCommit(false)
                                        .setKeyDeserializer("org.apache.kafka.common.serialization.StringDeserializer")
                                        .setValueDeserializer("org.apache.kafka.common.serialization.StringDeserializer")
                                        .build(),
                                Stream.of("flink-consumer01-topic").collect(Collectors.toSet())));

        SingleOutputStreamOperator<String> flatMapStream = consumerStreamSource
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String value, Collector<String> collector) throws Exception {
						// TODO some transformation
                        System.out.println("value:" + value);
                        collector.collect(value);
                    }
                });
```

#### source

```java
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
```
