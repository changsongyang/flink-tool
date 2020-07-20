package com.lzhpo.flinkrocketmq.config;

import com.lzhpo.common.BaseFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * RocketMQ Consumer Config
 *
 * @author Zhaopo Liu
 * @date 2020/7/20 17:14
 */
@Slf4j
public class RocketMqConsumerConfig extends BaseFactory<List<MessageExt>> {

    private String consumerGroup = "lzhpo_consumer_group_test";
    private String namesrvAddr = "localhost:9876";

    private String topic = "default_topic";

    /**
     * RocketMQ消费者，返回的是接收的此topic下的消息数据集合。
     *
     * @return {List<MessageExt>}
     */
    @Override
    public List<MessageExt> createFactory() {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(this.consumerGroup);
        consumer.setNamesrvAddr(namesrvAddr);
        // 设置 Consumer 第一次启动时从队列头部开始消费还是队列尾部开始消费
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        // 订阅指定topic下的所有消息
        try {
            consumer.subscribe(topic, "*");
        } catch (MQClientException e) {
            log.error("RocketMQ subscribe {} topic failed.", topic, e);
        }
        // 返回的消息集合
        List<MessageExt> msgList = new ArrayList<>();
        // 注册消息监听器
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext context) {
                // 添加返回消息集合数据
                msgList.addAll(list);
                //默认 list 里只有一条消息，可以通过设置参数来批量接收消息
                if (list != null) {
                    for (MessageExt ext : list) {
                        log.info("Topic={} receive new message {}", topic, new Date() + new String(ext.getBody(), StandardCharsets.UTF_8));
                    }
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        try {
            // 消费者对象在使用之前必须要调用start初始化
            consumer.start();
        } catch (MQClientException e) {
            log.error("Start rocketmq consumer failed.", e);
        }
        return msgList;
    }

    /**
     * Builder for {@link RocketMqConsumerConfig}
     *
     * @return Builder
     */
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private RocketMqConsumerConfig rocketMqConsumerConfig;

        public Builder setConsumerGroup(String consumerGroup) {
            rocketMqConsumerConfig.consumerGroup = consumerGroup;
            return this;
        }

        public Builder setNamesrvAddr(String namesrvAddr) {
            rocketMqConsumerConfig.namesrvAddr = namesrvAddr;
            return this;
        }

        public Builder setTopic(String topic) {
            rocketMqConsumerConfig.topic = topic;
            return this;
        }

        public RocketMqConsumerConfig build() {
            return rocketMqConsumerConfig;
        }
    }
}
