package com.lzhpo.flinkhive.config;

import com.lzhpo.common.BaseFactory;

import java.sql.*;

/**
 * HiveConnectionConfig
 *
 * @author Zhaopo Liu
 * @date 2020/6/20 03:14
 */
public class HiveConnectionConfig extends BaseFactory<Connection> {

    private String driverName = "org.apache.hive.jdbc.HiveDriver";
    private String url = "jdbc:hive2://localhost:10000/db_comprs";
    private String user = "hadoop";
    private String password = "";

    private Connection conn;

    @Override
    public Connection createFactory() {
        try {
            Class.forName(driverName);
            conn = DriverManager.getConnection(url,user,password);
        } catch (SQLException | ClassNotFoundException throwables) {
            throwables.printStackTrace();
        }
        return conn;
    }

    /**
     * This for {@link HiveConnectionConfig} build
     *
     * @return Builder
     */
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final HiveConnectionConfig hiveConnectionConfig;

        public Builder() {
            hiveConnectionConfig = new HiveConnectionConfig();
        }

        public Builder setDriverName(String driverName) {
            hiveConnectionConfig.driverName = driverName;
            return this;
        }

        public Builder setUrl(String url) {
            hiveConnectionConfig.url = url;
            return this;
        }

        public Builder setUsername(String user) {
            hiveConnectionConfig.user = user;
            return this;
        }

        public Builder setPassword(String password) {
            hiveConnectionConfig.password = password;
            return this;
        }

        public HiveConnectionConfig build() {
            return hiveConnectionConfig;
        }
    }
}
