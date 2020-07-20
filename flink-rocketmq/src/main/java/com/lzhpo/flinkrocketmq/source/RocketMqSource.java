package com.lzhpo.flinkrocketmq.source;

import com.lzhpo.flinkrocketmq.config.RocketMqConsumerConfig;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * RocketMq Source
 *
 * @author Zhaopo Liu
 * @date 2020/7/20 17:31
 */
public class RocketMqSource<OUT> extends RichSourceFunction<OUT> {

    /**
     * 反序列化
     */
    private DeserializationSchema<OUT> deserializationSchema;

    /**
     * RocketMQ Consumer Config
     */
    private RocketMqConsumerConfig rocketMqConsumerConfig;

    /**
     * RocketMQ的consumer
     */
    protected DefaultMQPushConsumer consumer;

    /**
     * 拿到的topic里面的数据
     */
    protected List<MessageExt> messageExtList;

    /**
     * 控制运行状态
     */
    protected boolean running = true;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        messageExtList = rocketMqConsumerConfig.createFactory();
    }

    @Override
    public void run(SourceContext<OUT> ctx) throws Exception {
        while (running) {
            // 反序列化
            OUT result = deserializationSchema.deserialize(messageExtList.toString().getBytes());
            ctx.collect(result);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (consumer != null) {
            consumer.shutdown();
        }
    }
}
