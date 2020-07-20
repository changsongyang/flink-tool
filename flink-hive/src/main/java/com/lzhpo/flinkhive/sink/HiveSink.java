package com.lzhpo.flinkhive.sink;

import com.lzhpo.flinkhive.config.HiveConnectionConfig;
import com.lzhpo.flinkhive.utils.MyHiveUtil;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * Kafka source sink to hive
 *
 * @author Zhaopo Liu
 * @date 2020/7/20 14:37
 */
public class HiveSink<IN> extends RichSinkFunction<IN> {

    /** 序列化 */
    private SerializationSchema<IN> schema;

    private HiveConnectionConfig hiveConnectionConfig;

    protected MyHiveUtil hiveUtil;

    public HiveSink(SerializationSchema<IN> serializationSchema,
                    HiveConnectionConfig hiveConnectionConfig) {
        this.schema = serializationSchema;
        this.hiveConnectionConfig = hiveConnectionConfig;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        hiveUtil = new MyHiveUtil(this.hiveConnectionConfig);
    }

    @Override
    public void close() throws Exception {
        super.close();
        hiveUtil.closeDB();
    }

    @Override
    public void invoke(IN value, Context context) throws Exception {

    }
}
