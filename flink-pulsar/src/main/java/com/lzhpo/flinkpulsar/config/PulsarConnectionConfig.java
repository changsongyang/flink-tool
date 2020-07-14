package com.lzhpo.flinkpulsar.config;

import com.lzhpo.common.BaseFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.*;

import java.util.concurrent.TimeUnit;

/**
 * Pulsar生产者、消费者连接
 *
 * @author lzhpo
 */
public class PulsarConnectionConfig<T> extends BaseFactory<T> {

    private String serviceUrl = "pulsar://localhost:6650";
    private String topic = "test-topic";
    private String token;
    private int batchingMaxPublishDelay = 10;
    private int sendTimeout = 10;
    private boolean blockIfQueueFull = true;
    //------------------------------------
    /** 订阅名称 */
    public static String subscriptionName = "test-sub";
    /** ackTimeout */
    public static long ackTimeout = 10;
    /** pulsar连接模式 */
    public static SubscriptionType subscriptionType = SubscriptionType.Exclusive;
    /** 消费消息之后是否立即删除？Pulsar底层默认ack之后就删除！默认：true */
    public static boolean autoDelData = true;
    /** 是否一直接受消息？默认：true */
    public static boolean alwaysReceive = true;

    @Deprecated
    @Override
    public T createFactory() {
        return null;
    }

    /**
     * Pulsar生产者
     *
     * @return Producer
     */
    public Producer<byte[]> createProducerFactory() throws PulsarClientException {
        if (StringUtils.isNotEmpty(token)) {
            return PulsarClient.builder()
                    .serviceUrl(serviceUrl)
                    // 发送 Token 等同于在网络上发送密码。 在连接 Pulsar 服务的整个过程中，最好始终使用 TLS 加密。
                    .authentication(AuthenticationFactory.token(token))
                    .build()
                    // 创建一个生产者
                    .newProducer()
                    .topic(topic)
                    // 批处理最大发布延迟
                    .batchingMaxPublishDelay(batchingMaxPublishDelay, TimeUnit.MILLISECONDS)
                    // 延迟发送
                    .sendTimeout(sendTimeout, TimeUnit.SECONDS)
                    // 如果队列已满则阻止
                    .blockIfQueueFull(blockIfQueueFull)
                    .create();
        } else {
            return PulsarClient.builder()
                    .serviceUrl(serviceUrl)
                    .build()
                    // 创建一个生产者
                    .newProducer()
                    .topic(topic)
                    // 批处理最大发布延迟
                    .batchingMaxPublishDelay(batchingMaxPublishDelay, TimeUnit.MILLISECONDS)
                    // 延迟发送
                    .sendTimeout(sendTimeout, TimeUnit.SECONDS)
                    // 如果队列已满则阻止
                    .blockIfQueueFull(blockIfQueueFull)
                    .create();
        }
    }

    /**
     * Pulsar消费者
     *
     * @return Consumer
     */
    public Consumer<byte[]> createConsumerFactory() throws PulsarClientException {
        if (StringUtils.isNotEmpty(token)) {
            return PulsarClient.builder()
                    .serviceUrl(serviceUrl)
                    // 发送 Token 等同于在网络上发送密码。 在连接 Pulsar 服务的整个过程中，最好始终使用 TLS 加密。
                    .authentication(AuthenticationFactory.token(token))
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
        } else {
            return PulsarClient.builder()
                    .serviceUrl(serviceUrl)
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
        }
    }

    /**
     * Builder for {@link PulsarConnectionConfig}
     * @return
     */
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private PulsarConnectionConfig pulsarConnectionConfig;

        public Builder setServerUrl(String serviceUrl) {
            pulsarConnectionConfig.serviceUrl = serviceUrl;
            return this;
        }

        public Builder setTopic(String topic) {
            pulsarConnectionConfig.topic = topic;
            return this;
        }

        public Builder setToken(String token) {
            pulsarConnectionConfig.token = token;
            return this;
        }

        public Builder setBatchingMaxPublishDelay(int batchingMaxPublishDelay) {
            pulsarConnectionConfig.batchingMaxPublishDelay = batchingMaxPublishDelay;
            return this;
        }

        public Builder setSendTimeout(int sendTimeout) {
            pulsarConnectionConfig.sendTimeout = sendTimeout;
            return this;
        }

        public Builder setBlockIfQueueFull(boolean blockIfQueueFull) {
            pulsarConnectionConfig.blockIfQueueFull = blockIfQueueFull;
            return this;
        }

        public PulsarConnectionConfig build() {
            return pulsarConnectionConfig;
        }
    }
}
