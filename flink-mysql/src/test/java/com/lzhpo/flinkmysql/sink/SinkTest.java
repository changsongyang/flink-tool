package com.lzhpo.flinkmysql.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink sink to mysql test
 *
 * @author lzhpo
 */
public class SinkTest {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 这里的值随便一个东西就行，主要是为了要使用DataStreamSource来使用sink
        DataStreamSource<String> dataStreamSource = env.fromElements("lzhpo");

        dataStreamSource.addSink(new FlinkMysqlSink<>(
                new SimpleStringSchema())
                .url("jdbc:mysql://localhost:3306/study-flink?useSSL=false&useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=CTT&characterEncoding=UTF-8&autoReconnect=true&failOverReadOnly=false")
                .username("root")
                .password("123456")
                .sql("INSERT INTO `study-flink`.`tb_transaction`(`transaction_id`, `card_number`, `terminal_id`, `transaction_date`, `transaction_time`, `transaction_type`, `amount`) VALUES (1, 111, 111, '2020-06-27 17:55:43', '17:55:48', 1, 234.11)"));

        env.execute("Flink sink mysql job");
    }

}
