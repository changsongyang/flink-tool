package com.lzhpo.flinkpulsar.sink;

import com.lzhpo.flinkpulsar.config.PulsarProducerConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;

/**
 * Pulsar Flink Sink
 *
 * @author Zhaopo Liu
 * @date 2020/6/20 03:14
 */
@Slf4j
public class FlinkPulsarProducer<IN> extends RichSinkFunction<IN> {

    /**
     * 生产者
     */
    protected Producer<byte[]> producer;

    /**
     * 序列化
     */
    private SerializationSchema<IN> schema;

    /**
     * pulsarProducerConfig {@link PulsarProducerConfig}
     */
    private PulsarProducerConfig pulsarProducerConfig;

    /**
     * 发送完就关闭连接
     */
    private boolean sendAndClose = true;

    public FlinkPulsarProducer(SerializationSchema<IN> schema,
                               PulsarProducerConfig pulsarProducerConfig, boolean sendAndClose) {
        this.schema = schema;
        this.pulsarProducerConfig = pulsarProducerConfig;
        this.sendAndClose = sendAndClose;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        producer = pulsarProducerConfig.createFactory();
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (producer != null) {
            producer.close();
        }
    }

    @Override
    public void invoke(IN value, Context context) throws Exception {
        byte[] result = schema.serialize(value);
        if (sendAndClose) {
            // 发送完关闭连接
            try {
                producer.send(result);
                log.info("The data {} send to pulsar successfully.", new String(result));
                close();
            } catch (PulsarClientException e) {
                e.printStackTrace();
            }
        } else {
            // 发送完不关闭连接
            producer.sendAsync(result)
                    .thenAccept(
                            msgId -> {
                                log.info(
                                        "The msgId [{}] data is [{}] send to pulsar successfully!",
                                        msgId,
                                        new String(result));
                            });
        }
    }

}
