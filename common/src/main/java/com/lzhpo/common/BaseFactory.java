package com.lzhpo.common;

/**
 * @author lzhpo
 */
public abstract class BaseFactory<T> {

    /**
     * 创建连接：消费者或生产者
     *
     * @return T
     */
    public abstract T createFactory();

}
