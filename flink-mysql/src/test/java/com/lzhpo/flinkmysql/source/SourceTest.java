package com.lzhpo.flinkmysql.source;

import com.google.gson.Gson;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * @author lzhpo
 */
public class SourceTest {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<ArrayList> dataStreamSource = env.addSource(
                FlinkMysqlSource.build(new SimpleStringSchema())
                .setUrl("jdbc:mysql://localhost:3306/study-flink?useSSL=false&useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=CTT&characterEncoding=UTF-8&autoReconnect=true&failOverReadOnly=false")
                .setUsername("root")
                .setPassword("123456")
                .setSql("select id,name,course,score from tb_student;"));

        dataStreamSource.flatMap(new FlatMapFunction<ArrayList, Student>() {
            @Override
            public void flatMap(ArrayList value, Collector<Student> collector) throws Exception {
                System.out.println("value：" +value);
                value.forEach(item -> {
                    Student student = new Gson().fromJson(String.valueOf(item), Student.class);
                    collector.collect(student);
                });
            }
        })
        .print();

        env.execute("Flink mysql source");
    }

}
