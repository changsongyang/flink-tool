package com.lzhpo.pulsarflink;

import com.lzhpo.pulsarflink.source.ConsumerConfigConstant;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import java.util.concurrent.TimeUnit;

/**
 * Pulsar消费者工厂
 * @author lzhpo
 */
public class ConsumerSourceFactory extends BasePulsarFactory<Consumer<byte[]>> {

    @Override
    public Consumer<byte[]> createPulsarFactory(PulsarClient pulsarClient)
            throws PulsarClientException {
        return PulsarClient.builder()
                .serviceUrl(ConsumerConfigConstant.PULSAR_SERVER_URL)
                // 发送 Token 等同于在网络上发送密码。 在连接 Pulsar 服务的整个过程中，最好始终使用 TLS 加密。
                .authentication(AuthenticationFactory.token(ConsumerConfigConstant.TOKEN))
                .build()
                // 构建一个消费者
                .newConsumer()
                // topic
                .topic(ConsumerConfigConstant.TOPIC)
                // 订阅名称
                .subscriptionName(ConsumerConfigConstant.SUBSCRIPTION_NAME)
                // 延迟ack，也就是延迟删除
                .ackTimeout(ConsumerConfigConstant.ACK_TIMEOUT, TimeUnit.SECONDS)
                // 订阅模式
                .subscriptionType(ConsumerConfigConstant.SUBSCRIPTION_TYPE)
                .subscribe();
    }
}
