package com.lzhpo.flinkpulsar.source;

import com.lzhpo.flinkpulsar.config.PulsarConsumerConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;

import java.util.concurrent.CompletableFuture;

/**
 * Flink使用Pulsar作为数据源
 *
 * @author Zhaopo Liu
 * @date 2020/6/20 03:14
 */
@Slf4j
public class FlinkPulsarConsumer<OUT> extends RichSourceFunction<OUT> implements ResultTypeQueryable<OUT> {

    /** consumer */
    protected Consumer<byte[]> consumer;


    /** 运行状态 */
    protected boolean running = true;

    /** 反序列化 */
    private DeserializationSchema<OUT> schema;

    /** pulsarConsumerConfig {@link PulsarConsumerConfig} */
    private PulsarConsumerConfig pulsarConsumerConfig;

    /** 消费消息之后是否立即删除？Pulsar底层默认ack之后就删除！默认：true */
    private boolean autoDelData = true;


    public FlinkPulsarConsumer(DeserializationSchema<OUT> schema,
                               PulsarConsumerConfig pulsarConsumerConfig, boolean autoDelData) {
        this.schema = schema;
        this.pulsarConsumerConfig = pulsarConsumerConfig;
        this.autoDelData = autoDelData;
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
        consumer = pulsarConsumerConfig.createFactory();
    }

    /**
     * DataStream 调用一次 run() 方法用来获取数据
     *
     * @param ctx
     * @throws Exception
     */
    @Override
    public void run(SourceContext<OUT> ctx) throws Exception {
        while (running) {
            // 异步接收消息
            CompletableFuture<Message<byte[]>> msg = consumer.receiveAsync();
            // msg.get().getData()是字节数组byte[]类型，要想看到字符串输出的话，需要 new String(msg.get().getData()) 转换一下
            log.info("The topic {} receive data {}", msg.get().getTopicName(), new String(msg.get().getData()));
            if (autoDelData) {
                // 序列化数据。Pulsar底层是数据格式是字节。
                byte[] dataByte = msg.get().getData();
                // Flink收集数据
                ctx.collect(schema.deserialize(dataByte));
                // 确认消息，以便消息代理可以将其删除
                consumer.acknowledge(msg.get());
            }
        }
    }

    /**
     * 取消一个Flink任务
     */
    @Override
    public void cancel() {
        running = false;
        log.info("The flink job is cancel.");
    }

    /**
     * 关闭连接
     */
    @Override
    public void close() {
        try {
            super.close();
            consumer.close();
        } catch (Exception e) {
            log.error("Close pulsar client connect fail.", e);
        }
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return schema.getProducedType();
    }
}
