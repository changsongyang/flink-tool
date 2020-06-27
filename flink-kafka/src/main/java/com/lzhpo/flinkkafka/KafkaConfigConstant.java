package com.lzhpo.flinkkafka;

import com.lzhpo.common.SendData;

import java.util.ArrayList;

/**
 * Kafka配置
 *
 * @author lzhpo
 */
public class KafkaConfigConstant {

    public static String BOOTSTRAP_SERVERS = "localhost:9092";
    public static String ACKS = "all";
    public static int RETRIES = 0;
    public static int BATCH_SIZE = 16384;
    public static int LINGER_MS = 1;
    public static int BUFFER_MEMORY = 33554432;

    //------------------生产者----------------------
    /** Topic */
    public static String TOPIC;
    /** 需要发送的消息数据 */
    public static SendData SEND_DATA = null;
    /** key序列化方式 */
    public static String KEY_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    /** value序列化方式 */
    public static String VALUE_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";

    //------------------消费者----------------------
    /** 订阅的topic */
    public static ArrayList<String> SUBSCRIBE_TOPICS;
    /** 消费者分组 */
    public static String GROUP_ID;
    /** 是否自动提交offset，建议不要自动提交。 */
    public static boolean ENABLE_AUTO_COMMIT = true;
    /** commit最大延迟 */
    public static int AUTO_COMMIT_INTERVAL_MS = 1000;
    /** 读取超时时间 */
    public static Integer POLL_TIMEOUT = 100;

    /** key序列化方式 */
    public static String KEY_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
    /** value序列化方式 */
    public static String VALUE_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
}
