package com.lzhpo.flinkmysql.source;

import com.lzhpo.common.modeltest.UserModelTest;
import com.lzhpo.flinkmysql.config.MysqlConnectionConfig;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 读取MySQL中的数据sink到Kafka
 *
 * @author Zhaopo Liu
 * @date 2020/6/20 03:14
 */
public class SourceTest {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<UserModelTest> userModelTestDataStreamSource = env.addSource(
                new FlinkKafkaSourceMysql<>(
                        new SimpleStringSchema(),
                        MysqlConnectionConfig.builder()
                                .setUrl("jdbc:mysql://localhost:3306/study-flink?useSSL=false&useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=CTT&characterEncoding=UTF-8&autoReconnect=true&failOverReadOnly=false")
                                .setUsername("root")
                                .setPassword("123456")
                                .build()
                )
        );

        userModelTestDataStreamSource
                .print()
                .setParallelism(2);

        env.execute("Flink mysql source");
    }
}
