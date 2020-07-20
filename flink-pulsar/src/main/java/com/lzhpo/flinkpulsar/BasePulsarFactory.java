package com.lzhpo.flinkpulsar;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

/**
 * Pulsar抽象连接工厂
 *
 * @author Zhaopo Liu
 * @date 2020/6/20 03:14
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
