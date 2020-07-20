package com.lzhpo.flinkmysql.user;

import com.google.gson.Gson;
import com.lzhpo.common.modeltest.UserModelTest;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author lzhpo
 */
public class KafkaUserProducerTest {

    public static final String broker_list = "192.168.200.109:9092";
    // kafka topic 需要和 flink 程序用同一个 topic
    public static final String topic = "flink-mysql-sink-topic";

    public static void writeToKafka() throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", broker_list);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        while(true) {
            UserModelTest user = UserModelTest.builder()
                    .setId((long) (1+Math.random()*(100000000-1+1)))
                    .setName("lzhpo" + (1+Math.random()*(100-1+1)))
                    .setLocation("广州" + (1+Math.random()*(100-1+1)))
                    .build();

            String userJson = new Gson().toJson(user);

            ProducerRecord<String, String> record = new ProducerRecord<>(topic,
                    null, null, userJson);
            producer.send(record);
            System.out.println("发送数据: " + userJson);

            Thread.sleep(5000);

            producer.flush();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        writeToKafka();
    }

}
