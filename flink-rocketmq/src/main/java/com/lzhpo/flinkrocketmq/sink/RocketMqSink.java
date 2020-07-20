package com.lzhpo.flinkrocketmq.sink;

import com.lzhpo.flinkrocketmq.config.RocketMqProducerConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

/**
 * RocketMq Sink
 *
 * @author Zhaopo Liu
 * @date 2020/7/20 15:27
 */
@Slf4j
public class RocketMqSink<IN> extends RichSinkFunction<IN> {

    /**
     * 序列化
     */
    private SerializationSchema<IN> schema;

    /**
     * RocketMQ生产者配置
     */
    private RocketMqProducerConfig rocketMqProducerConfig;

    /**
     * 需要保存消息的topic
     */
    private String topic;

    /**
     * producer
     */
    protected DefaultMQProducer producer;


    public RocketMqSink(SerializationSchema<IN> schema,
                        RocketMqProducerConfig rocketMqProducerConfig,
                        String topic) {
        this.schema = schema;
        this.rocketMqProducerConfig = rocketMqProducerConfig;
        this.topic = topic;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        producer = rocketMqProducerConfig.createFactory();
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (producer != null) {
            producer.shutdown();
        }
    }

    @Override
    public void invoke(IN value, Context context) throws Exception {
        byte[] result = schema.serialize(value);
        // 创建一条消息对象，指定其主题、标签和消息内容
        Message message = new Message(
                topic,
                result
        );
        SendResult sendResult = producer.send(message);
        log.info("The data [{}] send rocketmq topic [{}] result [{}].", value, topic, sendResult);
    }
}
