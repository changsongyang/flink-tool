package com.lzhpo.flinkmysql.sink;

import com.lzhpo.common.modeltest.UserModelTest;
import com.lzhpo.flinkmysql.config.MysqlConnectionConfig;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 读取Kafka中指定的topic数据sink到MySQL
 *
 * @author Zhaopo Liu
 * @date 2020/6/20 03:14
 */
public class SinkTest {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<UserModelTest> userModelTestDataStreamSource = env.fromElements(
                UserModelTest.builder()
                        .setId(123456787654L)
                        .setName("lzhpo")
                        .setLocation("广东广州")
                        .build()
        );

        // sink
        userModelTestDataStreamSource.addSink(
                new FlinkKafkaSinkMysql<>(
                        new SimpleStringSchema(),
                        MysqlConnectionConfig.builder()
                                .setUrl("jdbc:mysql://localhost:3306/study-flink?useSSL=false&useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=CTT&characterEncoding=UTF-8&autoReconnect=true&failOverReadOnly=false")
                                .setUsername("root")
                                .setPassword("123456")
                                .build()
                )
        );

        env.execute("Flink sink mysql job");
    }
}
