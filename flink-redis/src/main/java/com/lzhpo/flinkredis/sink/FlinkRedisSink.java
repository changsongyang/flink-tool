package com.lzhpo.flinkredis.sink;

import com.lzhpo.flinkredis.config.RedisConnectionConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;

/** @author <a href="https://www.lzhpo.com">Pop Liu</a> */
@Slf4j
public class FlinkRedisSink<IN> extends RichSinkFunction<HashMap<String, String>> {

  protected DeserializationSchema<IN> deserializationSchema;
  private final RedisConnectionConfig redisConnectionConfig;
  /** 插入数据过期时间(单位/秒) */
  private int expireSecond;

  /** jedis */
  protected Jedis jedis;

  public FlinkRedisSink(
      DeserializationSchema<IN> deserializationSchema,
      RedisConnectionConfig redisConnectionConfig,
      int expireSecond) {
    this.deserializationSchema = deserializationSchema;
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
   * @param map 需要插入到Redis中的数据key-value
   * @param context
   * @throws Exception
   */
  @Override
  public void invoke(HashMap<String, String> map, Context context) throws Exception {
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
