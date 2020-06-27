package com.lzhpo.flinkkafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * 自定义Kafka发送消息的回调函数
 *
 * @author lzhpo
 */
@Slf4j
public class KafkaCallback implements Callback {
    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        if (recordMetadata != null){
            System.out.println(recordMetadata.partition() + "---" +recordMetadata.offset());
            log.info("{} --- {}", recordMetadata.partition(), recordMetadata.offset());
        }
        if (e != null) {
            log.error("==>Send data to kafka fail.", e);
        }
    }
}
