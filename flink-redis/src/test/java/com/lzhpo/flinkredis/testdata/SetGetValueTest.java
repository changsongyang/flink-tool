package com.lzhpo.flinkredis.testdata;

import com.lzhpo.flinkredis.config.RedisConnectionConfig;
import org.junit.Test;
import redis.clients.jedis.Jedis;

/**
 * @author Zhaopo Liu
 * @date 2020/6/20 03:14
 */
public class SetGetValueTest {

    @Test
    public void SetDataRedis_Test() {
        RedisConnectionConfig redisConnectionConfig = RedisConnectionConfig.builder()
                .setRedisUrl("192.168.200.109")
                .setRedisPort(6379)
                .setPassWord("123456")
                .setDataBase(0)
                .build();
        Jedis jedis = redisConnectionConfig.createFactory();
        jedis.set("lzhpo", "https://www.lzhpo.com");
    }
}
