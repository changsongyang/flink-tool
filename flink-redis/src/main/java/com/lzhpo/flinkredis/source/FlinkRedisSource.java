package com.lzhpo.flinkredis.source;

import com.lzhpo.flinkredis.config.RedisConnectionConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import redis.clients.jedis.Jedis;

/**
 * @author Zhaopo Liu
 * @date 2020/6/20 03:14
 */
public class FlinkRedisSource<IN> extends RichSourceFunction<IN> {

    private DeserializationSchema<IN> schema;
    private RedisConnectionConfig redisConnectionConfig;

    /** get redis data key */
    private String key;

    /** jedis */
    protected Jedis jedis;

    protected boolean running = true;

    public FlinkRedisSource(DeserializationSchema<IN> deserializationSchema,
                            RedisConnectionConfig redisConnectionConfig, String key) {
        this.schema = deserializationSchema;
        this.redisConnectionConfig = redisConnectionConfig;
        this.key = key;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        jedis = redisConnectionConfig.createFactory();
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (jedis != null) {
            jedis.close();
        }
    }

    /**
     * TODO：实时监听某一个或多个key
     *
     * @param ctx
     * @throws Exception
     */
    @Override
    public void run(SourceContext<IN> ctx) throws Exception {
        String dataKey  = jedis.get(key);
        if (StringUtils.isNotEmpty(dataKey) && running) {
            try {
                IN result = schema.deserialize(dataKey.getBytes());
                ctx.collect(result);
            } finally {
                running = false;
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
