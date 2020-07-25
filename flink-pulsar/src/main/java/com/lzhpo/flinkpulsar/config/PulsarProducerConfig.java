package com.lzhpo.flinkpulsar.config;

import com.lzhpo.common.BaseFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import java.util.concurrent.TimeUnit;

/**
 * pulsar生产者配置
 *
 * @author Zhaopo Liu
 * @date 2020/7/25 22:12
 */
@Slf4j
public class PulsarProducerConfig extends BaseFactory<Producer<byte[]>> {

    /**
     * 连接地址
     */
    public String pulsarUrl = "pulsar://localhost:6650";

    /**
     * token：发送 Token 等同于在网络上发送密码。 在连接 Pulsar 服务的整个过程中，最好始终使用 TLS 加密。
     */
    public String pulsarToken;

    /**
     * pulsar的topic
     */
    private String topic = "default-test-topic";

    /**
     * 批处理延迟
     */
    private int batchingMaxPublishDelay = 10;

    /**
     * 发送延迟
     */
    private int sendTimeout = 10;

    /**
     * 队列已满是否阻止
     */
    private boolean blockIfQueueFull = true;

    /**
     * Pulsar生产者
     *
     * @return Producer
     */
    @Override
    public Producer<byte[]> createFactory() {
        if (StringUtils.isNotEmpty(pulsarToken)) {
            try {
                return PulsarClient.builder()
                        .serviceUrl(pulsarUrl)
                        // 发送 Token 等同于在网络上发送密码。 在连接 Pulsar 服务的整个过程中，最好始终使用 TLS 加密。
                        .authentication(AuthenticationFactory.token(pulsarToken))
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
            } catch (PulsarClientException e) {
                log.error("Connect pulsar client failed.", e);
                return null;
            }
        } else {
            try {
                return PulsarClient.builder()
                        .serviceUrl(pulsarUrl)
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
            } catch (PulsarClientException e) {
                log.error("Connect pulsar client failed.", e);
                return null;
            }
        }
    }

    /**
     * Builder for {@link PulsarProducerConfig}
     *
     * @return Builder
     */
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final PulsarProducerConfig pulsarProducerConfig;

        public Builder() {
            pulsarProducerConfig = new PulsarProducerConfig();
        }

        public Builder setPulsarUrl(String pulsarUrl) {
            pulsarProducerConfig.pulsarUrl = pulsarUrl;
            return this;
        }

        public Builder setPulsarToken(String pulsarToken) {
            pulsarProducerConfig.pulsarToken = pulsarToken;
            return this;
        }

        public Builder setTopic(String topic) {
            pulsarProducerConfig.topic = topic;
            return this;
        }

        public Builder setBatchingMaxPublishDelay(int batchingMaxPublishDelay) {
            pulsarProducerConfig.batchingMaxPublishDelay = batchingMaxPublishDelay;
            return this;
        }

        public Builder setSendTimeout(int sendTimeout) {
            pulsarProducerConfig.sendTimeout = sendTimeout;
            return this;
        }

        public Builder setBlockIfQueueFull(boolean blockIfQueueFull) {
            pulsarProducerConfig.blockIfQueueFull = blockIfQueueFull;
            return this;
        }

        public PulsarProducerConfig build() {
            return pulsarProducerConfig;
        }
    }
}
