package com.lzhpo.flinkkafka;

import com.lzhpo.common.BaseFactory;

import java.util.Properties;

/**
 * Kafka配置工厂
 *
 * @author lzhpo
 */
public class KafkaProducerFactory extends BaseFactory<Properties> {

    @Override
    public Properties createFactory() {
        Properties props = new Properties();
        props.put("bootstrap.servers", KafkaConfigConstant.BOOTSTRAP_SERVERS);
        props.put("acks", KafkaConfigConstant.ACKS);
        props.put("retries", KafkaConfigConstant.RETRIES);
        props.put("batch.size", KafkaConfigConstant.BATCH_SIZE);
        props.put("linger.ms", KafkaConfigConstant.LINGER_MS);
        props.put("buffer.memory", KafkaConfigConstant.BUFFER_MEMORY);
        props.put("key.serializer", KafkaConfigConstant.KEY_SERIALIZER);
        props.put("value.serializer", KafkaConfigConstant.VALUE_SERIALIZER);
        return props;
    }
}
