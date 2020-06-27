package com.lzhpo.flinkpulsar;

import com.lzhpo.common.SendData;
import com.lzhpo.flinkpulsar.sink.PulsarFlinkSink;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Date;
import java.util.Random;

/**
 * Flink Pulsar Sink Test
 *
 * @author lzhpo
 */
public class SinkTest {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 消息内容
        SendData sendData = SendData.builder()
                .id(new Random().nextLong())
                .body("Hello,lzhpo")
                .createDate(new Date())
                .creator("lzhpo")
                .builded();

        // env.fromElements将一系列元素作为数据源
        DataStreamSource<SendData> dataStreamSource = env.fromElements(sendData);

        PulsarFlinkSink<String> pulsarFlinkSink = PulsarFlinkSink.builder(new SimpleStringSchema())
                .setPulsarServerUrl("pulsar://192.168.200.109:6650")
                .setTopic("topic1")
                .setBatchingMaxPublishDelay(10)
                .setSendTimeout(10)
                // 发送一条消息的时候要同步发送
                .setSendAndClose(true)
                .setSendData(sendData);

        dataStreamSource
                .addSink(pulsarFlinkSink)
                .setParallelism(2);

        env.execute("Flink pulsar flink sink");
    }

}
