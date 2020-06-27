package com.lzhpo.pulsarflink;

import com.lzhpo.pulsarflink.sink.ProducerConfigConstant;
import com.lzhpo.pulsarflink.source.ConsumerConfigConstant;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import java.util.concurrent.TimeUnit;

/**
 * Pulsar生产者工厂
 *
 * @author lzhpo
 */
public class ProducerSinkFactory extends BasePulsarFactory<Producer<byte[]>> {

    @Override
    public Producer<byte[]> createPulsarFactory(PulsarClient pulsarClient)
            throws PulsarClientException {
        return PulsarClient.builder()
                .serviceUrl(ProducerConfigConstant.PULSAR_SERVER_URL)
                // 发送 Token 等同于在网络上发送密码。 在连接 Pulsar 服务的整个过程中，最好始终使用 TLS 加密。
                .authentication(AuthenticationFactory.token(ConsumerConfigConstant.TOKEN))
                .build()
                // 创建一个生产者
                .newProducer()
                .topic(ProducerConfigConstant.TOPIC)
                // 批处理最大发布延迟
                .batchingMaxPublishDelay(ProducerConfigConstant.BATCHING_MAX_PUBLISH_DELAY,
                        TimeUnit.MILLISECONDS)
                // 延迟发送
                .sendTimeout(ProducerConfigConstant.SEND_TIMEOUT, TimeUnit.SECONDS)
                // 如果队列已满则阻止
                .blockIfQueueFull(ProducerConfigConstant.BLOCK_IF_QUEUE_FULL)
                .create();
    }
}
