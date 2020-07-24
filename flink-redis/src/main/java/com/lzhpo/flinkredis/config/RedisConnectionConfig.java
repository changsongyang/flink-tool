package com.lzhpo.flinkredis.config;

import com.lzhpo.common.BaseFactory;
import lombok.Data;
import redis.clients.jedis.Jedis;

/**
 * @author Zhaopo Liu
 * @date 2020/6/20 03:14
 */
@Data
public class RedisConnectionConfig extends BaseFactory<Jedis> {

    /**
     * Redis的连接地址
     */
    private String host = "localhost";

    private int port = 6379;
    private String password;
    private int database = 0;

    /**
     * connect redis and createFactory
     *
     * @return Jedis
     */
    @Override
    public Jedis createFactory() {
        Jedis jedis = new Jedis(this.host, this.port);
        if (this.password != null) {
            jedis.auth(this.password);
        }
        jedis.select(this.database);
        return jedis;
    }

    /**
     * The Builder Class for {@link RedisConnectionConfig}.
     */
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final RedisConnectionConfig redisConnectionConfig;

        public Builder() {
            redisConnectionConfig = new RedisConnectionConfig();
        }

        public Builder setRedisUrl(String redisUrl) {
            redisConnectionConfig.host = redisUrl;
            return this;
        }

        public Builder setRedisPort(int redisPort) {
            redisConnectionConfig.port = redisPort;
            return this;
        }

        public Builder setPassWord(String passWord) {
            redisConnectionConfig.password = passWord;
            return this;
        }

        public Builder setDataBase(int dataBase) {
            redisConnectionConfig.database = dataBase;
            return this;
        }

        public RedisConnectionConfig build() {
            return redisConnectionConfig;
        }
    }
}
