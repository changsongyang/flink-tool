package com.lzhpo.flinkpulsar.config;

import com.lzhpo.common.BaseFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.*;

import java.util.concurrent.TimeUnit;

/**
 * Pulsar消费者配置
 *
 * @author Zhaopo Liu
 * @date 2020/7/25 21:29
 */
@Slf4j
public class PulsarConsumerConfig extends BaseFactory<Consumer<byte[]>> {

    /**
     * 连接地址
     */
    public String pulsarUrl = "pulsar://localhost:6650";

    /**
     * token：发送 Token 等同于在网络上发送密码。 在连接 Pulsar 服务的整个过程中，最好始终使用 TLS 加密。
     */
    public String pulsarToken;

    /**
     * topic
     */
    public String topic = "default-test-topic";

    /**
     * 订阅名称
     */
    public String subscriptionName = "default-test-sub";

    /**
     * ackTimeout
     */
    public long ackTimeout = 10;

    /**
     * pulsar连接模式
     */
    public SubscriptionType subscriptionType = SubscriptionType.Exclusive;

    @Override
    public Consumer<byte[]> createFactory() {
        if (StringUtils.isNotEmpty(pulsarToken)) {
            try {
                return PulsarClient.builder()
                        .serviceUrl(pulsarUrl)
                        // 发送 Token 等同于在网络上发送密码。 在连接 Pulsar 服务的整个过程中，最好始终使用 TLS 加密。
                        .authentication(AuthenticationFactory.token(pulsarToken))
                        .build()
                        // 构建一个消费者
                        .newConsumer()
                        // topic
                        .topic(topic)
                        // 订阅名称
                        .subscriptionName(subscriptionName)
                        // 延迟ack，也就是延迟删除
                        .ackTimeout(ackTimeout, TimeUnit.SECONDS)
                        // 订阅模式
                        .subscriptionType(subscriptionType)
                        .subscribe();
            } catch (PulsarClientException e) {
                log.error("Connect pulsar client failed.", e);
                return null;
            }
        } else {
            try {
                return PulsarClient.builder()
                        .serviceUrl(pulsarUrl)
                        .build()
                        // 构建一个消费者
                        .newConsumer()
                        // topic
                        .topic(topic)
                        // 订阅名称
                        .subscriptionName(subscriptionName)
                        // 延迟ack，也就是延迟删除
                        .ackTimeout(ackTimeout, TimeUnit.SECONDS)
                        // 订阅模式
                        .subscriptionType(subscriptionType)
                        .subscribe();
            } catch (PulsarClientException e) {
                log.error("Connect pulsar client failed.", e);
                return null;
            }
        }
    }

    /**
     * Builder for {@link PulsarConsumerConfig}
     *
     * @return Builder
     */
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final PulsarConsumerConfig pulsarConsumerConfig;

        public Builder() {
            pulsarConsumerConfig = new PulsarConsumerConfig();
        }

        public Builder setPulsarUrl(String pulsarUrl) {
            pulsarConsumerConfig.pulsarUrl = pulsarUrl;
            return this;
        }

        public Builder setPulsarToken(String pulsarToken) {
            pulsarConsumerConfig.pulsarToken = pulsarToken;
            return this;
        }

        public Builder setTopic(String topic) {
            pulsarConsumerConfig.topic = topic;
            return this;
        }

        public Builder setSubscriptionName(String subscriptionName) {
            pulsarConsumerConfig.subscriptionName = subscriptionName;
            return this;
        }

        public Builder setAckTimeout(long ackTimeout) {
            pulsarConsumerConfig.ackTimeout = ackTimeout;
            return this;
        }

        public Builder setSubscriptionType(SubscriptionType subscriptionType) {
            pulsarConsumerConfig.subscriptionType = subscriptionType;
            return this;
        }

        public PulsarConsumerConfig build() {
            return pulsarConsumerConfig;
        }
    }
}
