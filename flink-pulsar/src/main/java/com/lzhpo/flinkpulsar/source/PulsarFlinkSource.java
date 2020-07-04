package com.lzhpo.flinkpulsar.source;

import com.lzhpo.flinkpulsar.ConsumerSourceFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Preconditions;
import org.apache.pulsar.client.api.*;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Flink使用Pulsar作为数据源
 *
 * @author lzhpo
 */
@Slf4j
public class PulsarFlinkSource<T> extends RichSourceFunction<byte[]> {

  /** PulsarClient */
  PulsarClient pulsarClient;

  /** consumer */
  Consumer<byte[]> consumer;

  /** 序列化 */
  DeserializationSchema<T> deserializationSchema;

  /** @param deserializationSchema 序列化类型 */
  public PulsarFlinkSource(DeserializationSchema<T> deserializationSchema) {
    this.deserializationSchema = deserializationSchema;
  }

  /**
   * 与Pulsar建立连接
   *
   * @param parameters
   * @throws Exception
   */
  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    consumer = new ConsumerSourceFactory().createPulsarFactory(pulsarClient);
  }

  /**
   * DataStream 调用一次 run() 方法用来获取数据
   *
   * @param ctx
   * @throws Exception
   */
  @Override
  public void run(SourceContext<byte[]> ctx) throws Exception {
    if (ConsumerConfigConstant.ALWAYS_RECEIVE) {
      receiveAlways(ctx);
    } else {
      receiveOne(ctx);
    }
  }

  /** 取消一个Flink任务 */
  @Override
  public void cancel() {
    log.info("The flink job is cancel.");
  }

  /** 关闭连接 */
  @Override
  public void close() {
    try {
      super.close();
      consumer.close();
    } catch (Exception e) {
      log.error("Close pulsar client connect fail.", e);
    }
  }

  /**
   * 1.获取一次，就关闭会话
   *
   * @param ctx
   * @throws Exception
   */
  private void receiveOne(SourceContext<byte[]> ctx) throws Exception {
    // 异步接收
    CompletableFuture<Message<byte[]>> msg = consumer.receiveAsync();
    // msg.get().getData()是字节数组byte[]类型，要想看到字符串输出的话，需要 new String(msg.get().getData()) 转换一下
    log.info(
        "The topic [{}] receive data [{}].",
        msg.get().getTopicName(),
        new String(msg.get().getData()));
    if (ConsumerConfigConstant.AUTO_DEL_DATA) {
      // 序列化数据。Pulsar底层是数据格式是字节。
      byte[] dataByte = msg.get().getData();
      // Flink收集数据
      ctx.collect(dataByte);
      // 确认消息，以便消息代理可以将其删除
      consumer.acknowledge(msg.get());
    }
    close();
  }

  /**
   * 2.持续接收消息
   *
   * @param ctx
   * @throws InterruptedException
   * @throws ExecutionException
   * @throws PulsarClientException
   */
  private void receiveAlways(SourceContext<byte[]> ctx)
      throws InterruptedException, ExecutionException, IOException {
    System.out.println("receiveAlways...");
    // 用来异步获取，保持回话
    do {
      // 异步接收消息
      CompletableFuture<Message<byte[]>> msg = consumer.receiveAsync();
      // msg.get().getData()是字节数组byte[]类型，要想看到字符串输出的话，需要 new String(msg.get().getData()) 转换一下
      log.info(
          "The topic [{}] receive data [{}].",
          msg.get().getTopicName(),
          new String(msg.get().getData()));
      if (ConsumerConfigConstant.AUTO_DEL_DATA) {
        // 序列化数据。Pulsar底层是数据格式是字节。
        byte[] dataByte = msg.get().getData();
        // Flink收集数据
        ctx.collect(dataByte);
        // 确认消息，以便消息代理可以将其删除
        consumer.acknowledge(msg.get());
      }
    } while (true);
  }

  /**
   * 构建一个PulsarSource
   *
   * @param deserializationSchema the deserializer used to convert between Pulsar's byte messages
   *     and Flink's objects.
   * @return a builder
   */
  public static <T> PulsarFlinkSource<T> builder(DeserializationSchema<T> deserializationSchema) {
    Preconditions.checkNotNull(deserializationSchema, "deserializationSchema cannot be null");
    return new PulsarFlinkSource<>(deserializationSchema);
  }

  public PulsarFlinkSource<T> setPulsarServerUrl(String pulsarServerUrl) {
    ConsumerConfigConstant.PULSAR_SERVER_URL = pulsarServerUrl;
    return this;
  }

  public PulsarFlinkSource<T> setTopic(String topic) {
    ConsumerConfigConstant.TOPIC = topic;
    return this;
  }

  public PulsarFlinkSource<T> setSubscriptionName(String subscriptionName) {
    ConsumerConfigConstant.SUBSCRIPTION_NAME = subscriptionName;
    return this;
  }

  public PulsarFlinkSource<T> setAckTimeout(long ackTimeout) {
    ConsumerConfigConstant.ACK_TIMEOUT = ackTimeout;
    return this;
  }

  public PulsarFlinkSource<T> setSubscriptionType(SubscriptionType subscriptionType) {
    ConsumerConfigConstant.SUBSCRIPTION_TYPE = subscriptionType;
    return this;
  }

  public PulsarFlinkSource<T> setAutoDelData(boolean autoDelData) {
    ConsumerConfigConstant.AUTO_DEL_DATA = autoDelData;
    return this;
  }

  public PulsarFlinkSource<T> setAlwaysReceive(boolean alwaysReceive) {
    ConsumerConfigConstant.ALWAYS_RECEIVE = alwaysReceive;
    return this;
  }
}
