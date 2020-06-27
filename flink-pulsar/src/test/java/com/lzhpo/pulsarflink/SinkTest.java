package com.lzhpo.pulsarflink;

import com.lzhpo.pulsarflink.sink.PulsarFlinkSink;
import com.lzhpo.pulsarflink.sink.SendData;
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
                .pulsarServerUrl("pulsar://192.168.200.109:6650")
                .topic("topic1")
                .batchingMaxPublishDelay(10)
                .sendTimeout(10)
                // 发送一条消息的时候要同步发送
                .sendAndClose(true)
                .sendData(null);

        dataStreamSource
                .addSink(pulsarFlinkSink)
                .setParallelism(2);

        env.execute("Flink pulsar flink sink");
    }

}
