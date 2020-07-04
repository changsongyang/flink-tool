package com.lzhpo.flinkpulsar.sink;

import com.lzhpo.common.SendData;
import com.lzhpo.flinkpulsar.ProducerSinkFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Preconditions;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

/**
 * Pulsar Flink Sink
 *
 * @author lzhpo
 */
@Slf4j
public class PulsarFlinkSink<T> extends RichSinkFunction<SendData> {

  /** pulsar客户端 */
  PulsarClient pulsarClient;

  /** 生产者 */
  Producer<byte[]> producer;

  /** 序列化 */
  DeserializationSchema<T> deserializationSchema;

  /**
   * 序列化
   *
   * @param deserializationSchema deserializationSchema
   */
  public PulsarFlinkSink(DeserializationSchema<T> deserializationSchema) {
    this.deserializationSchema = deserializationSchema;
  }

  /**
   * open 在这里做连接操作
   *
   * @param parameters
   */
  @Override
  public void open(Configuration parameters) {
    try {
      super.open(parameters);
      producer = new ProducerSinkFactory().createPulsarFactory(pulsarClient);
      log.info("-----------producer parameter configuration-----------");
      log.info("pulsar_server_url: " + ProducerConfigConstant.PULSAR_SERVER_URL);
      log.info("topic: " + ProducerConfigConstant.TOPIC);
      log.info("batching_max_publish_delay: " + ProducerConfigConstant.BATCHING_MAX_PUBLISH_DELAY);
      log.info("send_timeout: " + ProducerConfigConstant.SEND_TIMEOUT);
      log.info("block_if_queue_full: " + ProducerConfigConstant.BLOCK_IF_QUEUE_FULL);
      log.info("send_data: " + ProducerConfigConstant.SEND_DATA);
      log.info("send_and_close: " + ProducerConfigConstant.SEND_AND_CLOSE);
      log.info("-----------------------------------------------------");
    } catch (Exception e) {
      log.error("Connect pulsar client fail!", e);
    }
  }

  /** close */
  @Override
  public void close() {
    try {
      super.close();
      producer.close();
    } catch (Exception e) {
      log.error("Close pulsar client connect fail.", e);
    }
  }

  /**
   * invoke
   *
   * @param sendData 发送的消息数据
   * @param context context
   */
  @Override
  public void invoke(SendData sendData, Context context) {
    if (sendData.getBody() != null) {
      String sendDataStr = String.valueOf(sendData);
      if (!ProducerConfigConstant.SEND_AND_CLOSE) {
        // 发送完不关闭连接
        producer
            .sendAsync(sendDataStr.getBytes())
            .thenAccept(
                msgId -> {
                  log.info(
                      "The msgId [{}] data is [{}] send to pulsar successfully!",
                      msgId,
                      sendDataStr);
                });
      } else {
        // 发送完关闭连接
        try {
          producer.send(sendDataStr.getBytes());
          log.info("The data is [{}] send to pulsar successfully!", sendDataStr);
          close();
        } catch (PulsarClientException e) {
          e.printStackTrace();
        }
      }
    } else {
      log.warn("Please set the SendData body!");
    }
  }

  /**
   * 构建一个PulsarSource
   *
   * @param deserializationSchema the deserializer used to convert between Pulsar's byte messages
   *     and Flink's objects.
   * @return a builder
   */
  public static <T> PulsarFlinkSink<T> builder(DeserializationSchema<T> deserializationSchema) {
    Preconditions.checkNotNull(deserializationSchema, "deserializationSchema cannot be null");
    return new PulsarFlinkSink<>(deserializationSchema);
  }

  public PulsarFlinkSink<T> setPulsarServerUrl(String pulsarServerUrl) {
    ProducerConfigConstant.PULSAR_SERVER_URL = pulsarServerUrl;
    return this;
  }

  public PulsarFlinkSink<T> setTopic(String topic) {
    ProducerConfigConstant.TOPIC = topic;
    return this;
  }

  public PulsarFlinkSink<T> setBatchingMaxPublishDelay(int batchingMaxPublishDelay) {
    ProducerConfigConstant.BATCHING_MAX_PUBLISH_DELAY = batchingMaxPublishDelay;
    return this;
  }

  public PulsarFlinkSink<T> setSendTimeout(int sendTimeout) {
    ProducerConfigConstant.SEND_TIMEOUT = sendTimeout;
    return this;
  }

  public PulsarFlinkSink<T> setSendAndClose(boolean sendAndClone) {
    ProducerConfigConstant.SEND_AND_CLOSE = sendAndClone;
    return this;
  }

  public PulsarFlinkSink<T> setSendData(SendData sendData) {
    ProducerConfigConstant.SEND_DATA = sendData;
    return this;
  }
}
