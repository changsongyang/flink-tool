package com.lzhpo.flinkredis.sink;

import com.google.gson.Gson;
import com.lzhpo.flinkredis.config.RedisConnectionConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Zhaopo Liu
 * @date 2020/6/20 03:14
 */
@Slf4j
public class RedisSink<IN> extends RichSinkFunction<IN> {

    /** 序列化 */
    private SerializationSchema<IN> schema;
    private final RedisConnectionConfig redisConnectionConfig;
    /**
     * 插入数据过期时间(单位/秒)
     */
    private int expireSecond;

    /**
     * jedis
     */
    protected Jedis jedis;

    public RedisSink(
            SerializationSchema<IN> deserializationSchema,
            RedisConnectionConfig redisConnectionConfig,
            int expireSecond) {
        this.schema = deserializationSchema;
        this.redisConnectionConfig = redisConnectionConfig;
        this.expireSecond = expireSecond;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        log.info("parameters:{}", parameters);
        super.open(parameters);
        log.info("redisConnectionConfig:{}", redisConnectionConfig);
        this.jedis = redisConnectionConfig.createFactory();
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (this.jedis != null) {
            jedis.close();
            log.info("Already close.");
        }
    }

    /**
     * 执行业务方法
     *
     * @param value
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(IN value, Context context) throws Exception {
        // 序列化
        byte[] bytes = schema.serialize(value);
        // 字节数组转换为HashMap
        HashMap<String, String> map = new Gson().fromJson(new String(bytes), HashMap.class);
        for (Map.Entry<String, String> entry : map.entrySet()) {
            String insertResult = "";
            // 设置过期时间
            if (this.expireSecond != 0) {
                // NX是不存在时才set， XX是存在时才set， EX是秒，PX是毫秒
                insertResult = jedis.set(entry.getKey(), entry.getValue(), "NX", "EX", this.expireSecond);
            } else {
                // 永久存储
                insertResult = jedis.set(entry.getKey(), entry.getValue());
            }
            log.info("insert redis data,key={},value={}", entry.getKey(), entry.getValue());
            log.info("insertResult:{}", insertResult);
        }
    }
}
