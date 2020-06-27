package com.lzhpo.flinkkafka;

import com.lzhpo.common.BaseFactory;

import java.util.Properties;

/**
 * Kafka配置工厂
 *
 * @author lzhpo
 */
public class KafkaConsumerFactory extends BaseFactory<Properties> {

    @Override
    public Properties createFactory() {
        Properties props = new Properties();
        props.put("bootstrap.servers", KafkaConfigConstant.BOOTSTRAP_SERVERS);
        props.put("group.id", KafkaConfigConstant.GROUP_ID);
        props.put("enable.auto.commit", KafkaConfigConstant.ENABLE_AUTO_COMMIT);
        props.put("auto.commit.interval.ms", KafkaConfigConstant.AUTO_COMMIT_INTERVAL_MS);
        props.put("key.deserializer", KafkaConfigConstant.KEY_DESERIALIZER);
        props.put("value.deserializer", KafkaConfigConstant.VALUE_DESERIALIZER);
        return props;
    }
}
