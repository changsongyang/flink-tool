package com.lzhpo.flinkhbase.sink;

import com.lzhpo.flinkhbase.config.HbaseConnectionConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;

/**
 * @author Zhaopo Liu
 * @date 2020/6/20 03:14
 */
@Slf4j
public class HbaseSink<IN> extends RichSinkFunction<IN> {

    /** 序列化 */
    private SerializationSchema<IN> schema;

    private HbaseConnectionConfig hbaseConnectionConfig;

    protected Connection connection;

    public HbaseSink(SerializationSchema<IN> serializationSchema,
                     HbaseConnectionConfig hbaseConnectionConfig) {
        this.schema = serializationSchema;
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
    public void invoke(IN value, Context context) throws Exception {

    }
}
