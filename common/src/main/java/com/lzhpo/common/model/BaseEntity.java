package com.lzhpo.common.model;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.util.logging.Logger;

/**
 * BaseEntity
 *
 * @author Zhaopo Liu
 * @date 2020/7/25 11:33
 */
public class BaseEntity<T> implements Serializable {

    Logger log = Logger.getLogger(BaseEntity.class.getName());

    /**
     * builder for {@link BaseEntity}
     *
     * @return Builder
     */
    public Builder builder() {
        return new Builder();
    }

    public class Builder {
        private T t;

        public Builder() {
            // Get the class generic type to t
            t = (T) getClassType();
            System.out.println("t:" +t);

        }

        public T build() {
            return t;
        }
    }

    /**
     * Get the class generic type
     *
     * @return the class generic type
     */
    public Class getClassType() {
        return (Class<T>) ((ParameterizedType) getClass()
                .getGenericSuperclass()).getActualTypeArguments()[0];
    }

    /**
     *
     * <pre>{@code clazz.newInstance()}</pre>
     *
     * Already deprecated after jdk9.
     *
     * Can be replaced by
     *
     * <pre>{@code clazz.getDeclaredConstructor().newInstance()}</pre>
     *
     */
    public void getClassTypeArgs() {
        Object ot = null;
        try {
            ot = Class.forName(getClassType().getName()).getDeclaredConstructor().newInstance();
        } catch (InstantiationException | InvocationTargetException | NoSuchMethodException | IllegalAccessException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        Class c = ot.getClass();
        // 获取所有私有成员变量
        Field[] fields = c.getDeclaredFields();
        // 取消每个属性的安全检查
        for(Field f:fields){
            f.setAccessible(true);
        }
        // 遍历私有成员变量
        for (Field f:fields) {
            log.info("field:" +f.getName());
        }
    }
}