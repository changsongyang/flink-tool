package com.lzhpo.flinkmysql.test;

import lombok.Data;

import java.io.Serializable;

/**
 * 测试
 * @author lzhpo
 */
@Data
public class User implements Serializable {
    private Long id;
    private String name;
    private String location;

    /**
     * builder
     *
     * @return Builder
     */
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private User user;

        public Builder() {
            user = new User();
        }

        public Builder setId(Long id) {
            user.id = id;
            return this;
        }

        public Builder setName(String name) {
            user.name = name;
            return this;
        }

        public Builder setLocation(String location) {
            user.location = location;
            return this;
        }

        public User build() {
            return user;
        }
    }
}
