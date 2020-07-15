package com.lzhpo.flinkhbase.sink;

import com.lzhpo.flinkhbase.config.HbaseConnectionConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;

/**
 * @author lzhpo
 */
@Slf4j
public class FlinkKafkaSinkHbase<T> extends RichSinkFunction<String> {

    private DeserializationSchema<T> deserializationSchema;
    private HbaseConnectionConfig hbaseConnectionConfig;

    protected Connection connection;

    public FlinkKafkaSinkHbase(DeserializationSchema<T> deserializationSchema,
                               HbaseConnectionConfig hbaseConnectionConfig) {
        this.deserializationSchema = deserializationSchema;
        this.hbaseConnectionConfig = hbaseConnectionConfig;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = hbaseConnectionConfig.createFactory();
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            connection.close();
        }
    }

    @Override
    public void invoke(String value, Context context) throws Exception {

    }
}
