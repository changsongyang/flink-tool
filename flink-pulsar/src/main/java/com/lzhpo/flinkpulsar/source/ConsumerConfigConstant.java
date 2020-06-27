package com.lzhpo.flinkpulsar.source;

import org.apache.pulsar.client.api.SubscriptionType;

/**
 * 消费者相关参数配置
 *
 * @author lzhpo
 */
public class ConsumerConfigConstant {

    /** 连接地址 */
    public static String PULSAR_SERVER_URL = "pulsar://localhost:6650";

    /** token：发送 Token 等同于在网络上发送密码。 在连接 Pulsar 服务的整个过程中，最好始终使用 TLS 加密。 */
    public static String TOKEN;

    /** topic */
    public static String TOPIC = "topic1";

    /** 订阅名称 */
    public static String SUBSCRIPTION_NAME = "test-sub";

    /** ackTimeout */
    public static long ACK_TIMEOUT = 10;

    /** pulsar连接模式 */
    public static SubscriptionType SUBSCRIPTION_TYPE = SubscriptionType.Exclusive;

    /** 消费消息之后是否立即删除？Pulsar底层默认ack之后就删除！默认：true */
    public static boolean AUTO_DEL_DATA = true;

    /** 是否一直接受消息？默认：true */
    public static boolean ALWAYS_RECEIVE = true;

}
