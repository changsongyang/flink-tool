package com.lzhpo.flinkmysql.config;

import com.lzhpo.common.BaseFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @author lzhpo
 */
public class MysqlConnectionConfig extends BaseFactory<Connection> {

    private String url = "jdbc:mysql://localhost:3306/study-flink?useSSL=false&useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=CTT&characterEncoding=UTF-8&autoReconnect=true&failOverReadOnly=false";
    private String username = "root";
    private String password = "123456";

    protected String driver = "com.mysql.jdbc.Driver";
    protected Connection connection;

    @Override
    public Connection createFactory() {
        try {
            Class.forName(driver);
            connection =
                    DriverManager.getConnection(url, username, password);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }

    /**
     * This for {@link MysqlConnectionConfig} build
     *
     * @return Builder
     */
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final MysqlConnectionConfig mysqlConnectionConfig;

        public Builder() {
            mysqlConnectionConfig = new MysqlConnectionConfig();
        }

        public Builder setUrl(String url) {
            mysqlConnectionConfig.url = url;
            return this;
        }

        public Builder setUsername(String username) {
            mysqlConnectionConfig.username = username;
            return this;
        }

        public Builder setPassword(String password) {
            mysqlConnectionConfig.password = password;
            return this;
        }

        public MysqlConnectionConfig build() {
            return mysqlConnectionConfig;
        }
    }

}
