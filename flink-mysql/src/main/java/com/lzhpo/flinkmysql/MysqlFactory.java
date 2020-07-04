package com.lzhpo.flinkmysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * mysql连接工厂
 *
 * @author lzhpo
 */
public class MysqlFactory {

  String driver = "com.mysql.jdbc.Driver";
  Connection connection;

  public Connection createMysqlFactory() {
    try {
      Class.forName(driver);
      connection =
          DriverManager.getConnection(
              MysqlConfigConstant.URL, MysqlConfigConstant.USERNAME, MysqlConfigConstant.PASSWORD);
    } catch (ClassNotFoundException | SQLException e) {
      e.printStackTrace();
    }
    return connection;
  }
}
