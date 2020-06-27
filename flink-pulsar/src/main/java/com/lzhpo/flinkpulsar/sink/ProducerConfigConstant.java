package com.lzhpo.flinkpulsar.sink;

/**
 * 生产者相关参数配置
 *
 * @author lzhpo
 */
public class ProducerConfigConstant {

    /** 连接地址 */
    public static String PULSAR_SERVER_URL = "pulsar://localhost:6650";
    /** topic */
    public static String TOPIC = "topic1";

    /** token：发送 Token 等同于在网络上发送密码。 在连接 Pulsar 服务的整个过程中，最好始终使用 TLS 加密。 */
    public static String TOKEN;

    /** 批处理最大发布延迟 */
    public static int BATCHING_MAX_PUBLISH_DELAY = 10;
    /** 延迟发送 */
    public static int SEND_TIMEOUT = 10;
    /** 如果队列已满则阻止 */
    public static boolean BLOCK_IF_QUEUE_FULL = true;
    /** 发送消息的内容 */
    public static SendData SEND_DATA = null;
    /** 是否发送完就关闭 */
    public static boolean SEND_AND_CLOSE = false;

}
