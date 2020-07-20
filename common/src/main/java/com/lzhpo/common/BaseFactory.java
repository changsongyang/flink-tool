package com.lzhpo.common;

import java.io.Serializable;

/**
 * BaseFactory：必须实现序列化，否则无法执行 Flink Job
 *
 * @author Zhaopo Liu
 * @date 2020/6/20 03:14
 */
public abstract class BaseFactory<T> implements Serializable {

  /**
   * createFactory
   *
   * @return <T> T
   */
  public abstract T createFactory();
}
