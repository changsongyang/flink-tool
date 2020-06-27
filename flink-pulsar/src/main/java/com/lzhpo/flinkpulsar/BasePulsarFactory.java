package com.lzhpo.pulsarflink;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

/**
 * Pulsar抽象连接工厂
 *
 * @author lzhpo
 */
public abstract class BasePulsarFactory<T> {

    /**
     * 创建连接：消费者或生产者
     *
     * @param pulsarClient pulsarClient
     * @return T
     * @throws PulsarClientException PulsarClientException
     */
    abstract T createPulsarFactory(PulsarClient pulsarClient) throws PulsarClientException;

}
