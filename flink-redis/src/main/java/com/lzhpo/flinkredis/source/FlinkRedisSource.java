package com.lzhpo.flinkredis.source;

import com.lzhpo.flinkredis.config.RedisConnectionConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import redis.clients.jedis.Jedis;

import java.util.HashMap;

/**
 * @author lzhpo
 */
public class FlinkRedisSource<IN> extends RichSourceFunction<HashMap<String, String>> {

    private DeserializationSchema<IN> deserializationSchema;
    private RedisConnectionConfig redisConnectionConfig;

    /** get redis data key */
    private String key;

    /** jedis */
    protected Jedis jedis;

    protected boolean running = true;

    public FlinkRedisSource(DeserializationSchema<IN> deserializationSchema,
                            RedisConnectionConfig redisConnectionConfig, String key) {
        this.deserializationSchema = deserializationSchema;
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
    public void run(SourceContext<HashMap<String, String>> ctx) throws Exception {
        String result = jedis.get(key);
        if (StringUtils.isNotEmpty(result) && running) {
            try {
                HashMap<String, String> hashMap = new HashMap<>(1);
                hashMap.put(key, result);
                ctx.collect(hashMap);
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
