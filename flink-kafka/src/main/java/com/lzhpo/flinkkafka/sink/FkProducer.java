package com.lzhpo.flinkkafka.sink;

import com.lzhpo.common.utils.SaveDataToDiskUtil;
import com.lzhpo.flinkkafka.config.KafkaProducerConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Kafka Sink
 *
 * TODO：Kafka数据倾斜的情况、Kafka事务(https://www.infoq.cn/article/kafka-analysis-part-8)
 *
 * <p>https://blog.csdn.net/learn_tech/article/details/80923308
 *
 * @author Zhaopo Liu
 * @date 2020/6/20 03:14
 */
@Slf4j
public class FkProducer<IN> extends RichSinkFunction<IN> {

    /** 序列化 */
    private SerializationSchema<IN> schema;

    /** key */
    private String key;

    /** topic */
    private String topic;

    /** kafkaProducerConfig {@link KafkaProducerConfig} */
    private KafkaProducerConfig kafkaProducerConfig;

    /** Kafka生产者 {@link KafkaProducer} */
    protected KafkaProducer<String, String> producer;

    public FkProducer(SerializationSchema<IN> schema, String key,
                      String topic, KafkaProducerConfig kafkaProducerConfig) {
        this.schema = schema;
        this.key = key;
        this.topic = topic;
        this.kafkaProducerConfig = kafkaProducerConfig;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        log.info("parameters:{}", parameters);
        producer = kafkaProducerConfig.createFactory();
        // 初始化事务
        producer.initTransactions();
        // 列出topic的相关信息
        List<PartitionInfo> partitionInfos;
        partitionInfos = producer.partitionsFor(topic);
        partitionInfos.forEach(System.out::println);
        // 开始事务
        producer.beginTransaction();
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (producer != null) {
            producer.close();
            log.warn("producer already close.");
        }
    }

    /**
     * 1.key != null
     * 按照key进行哈希，相同key去一个partition。（如果扩展了partition的数量那么就不能保证了）
     *
     * 2.key == null
     * 当key=null时，kafka是先从缓存中取分区号，然后判断缓存的值是否为空，如果不为空，就将消息存到这个分区，
     * 否则重新计算要存储的分区，并将分区号缓存起来，供下次使用。
     *
     * 失败的数据会持久化在磁盘{@code "/kafka-producer/fail-data/"+ LocalDateTime.now()+".txt"}
     *
     * @param value
     * @param context
     */
    @Override
    public void invoke(IN value, Context context) {
        String result = new String(schema.serialize(value));
        // 发送
        producer.send(
                new ProducerRecord<>(topic, key, result),
                (metadata, exception) -> {
                    if (exception == null) {
                        // 数据发送及 Offset 发送均成功的情况下，提交事务
                        producer.commitTransaction();
                        log.info("send data [{}] to kafka successfully.", result);
                    } else {
                        // 数据发送或者 Offset 发送出现异常时，终止事务
                        producer.abortTransaction();
                        log.error("send data [{}] to kafka failed.", result, exception);
                        // 将失败的数据持久化到磁盘
                        SaveDataToDiskUtil.saveStringToDisk(result,
                                "/kafka-producer/fail-data/"+ LocalDateTime.now()+".txt");
                    }
                }
        );
    }

}
