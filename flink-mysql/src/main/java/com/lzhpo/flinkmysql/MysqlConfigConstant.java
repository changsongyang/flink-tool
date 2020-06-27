package com.lzhpo.flinkmysql;

/**
 * MySQL连接以及执行的sql配置
 *
 * @author lzhpo
 */
public class MysqlConfigConstant {
    public static String URL = "jdbc:mysql://localhost:3306/study-flink?useSSL=false&useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=CTT&characterEncoding=UTF-8&autoReconnect=true&failOverReadOnly=false";
    public static String USERNAME  = "root";
    public static String PASSWORD = "123456";

    /** 需要执行的sql */
    public static String SQL;
}
