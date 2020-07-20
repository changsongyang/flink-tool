package com.lzhpo.flinkhive.sink;

import com.lzhpo.common.modeltest.UserModelTest;
import com.lzhpo.flinkhive.config.HiveConnectionConfig;
import com.lzhpo.flinkhive.utils.MyHiveUtil;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * Kafka source sink to hive
 *
 * @author Zhaopo Liu
 * @date 2020/7/20 14:37
 */
public class FlinkKafkaSinkHive<T> extends RichSinkFunction<UserModelTest> {

    private DeserializationSchema<T> deserializationSchema;
    private HiveConnectionConfig hiveConnectionConfig;

    protected MyHiveUtil hiveUtil;

    public FlinkKafkaSinkHive(DeserializationSchema<T> deserializationSchema,
                              HiveConnectionConfig hiveConnectionConfig) {
        this.deserializationSchema = deserializationSchema;
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
    public void invoke(UserModelTest value, Context context) throws Exception {

    }
}
