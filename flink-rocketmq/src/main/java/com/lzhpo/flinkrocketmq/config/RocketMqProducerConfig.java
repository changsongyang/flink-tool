package com.lzhpo.flinkrocketmq.config;

import com.lzhpo.common.BaseFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;

/**
 * RocketMq Producer Config
 *
 * @author Zhaopo Liu
 * @date 2020/7/20 15:15
 */
@Slf4j
public class RocketMqProducerConfig extends BaseFactory<DefaultMQProducer> {

    private String producerGroup = "lzhpo_producer_group_test";
    private String namesrvAddr = "localhost:9876";

    @Override
    public DefaultMQProducer createFactory() {
        DefaultMQProducer mqProducer = new DefaultMQProducer(producerGroup);
        mqProducer.setNamesrvAddr(namesrvAddr);
        try {
            mqProducer.start();
        } catch (MQClientException e) {
            log.error("初始化RocketMQ生产者失败！", e);
        }
        return mqProducer;
    }

    /**
     * Builder for {@link RocketMqProducerConfig}
     *
     * @return Builder
     */
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private RocketMqProducerConfig rocketMqProducerConfig;

        public Builder setProducerGroup(String producerGroup) {
            rocketMqProducerConfig.producerGroup = producerGroup;
            return this;
        }

        public Builder setNamesrvAddr(String namesrvAddr) {
            rocketMqProducerConfig.namesrvAddr = namesrvAddr;
            return this;
        }

        public RocketMqProducerConfig build() {
            return rocketMqProducerConfig;
        }
    }
}
