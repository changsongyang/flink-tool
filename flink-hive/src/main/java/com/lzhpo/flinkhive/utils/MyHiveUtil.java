package com.lzhpo.flinkhive.utils;

import com.lzhpo.flinkhive.config.HiveConnectionConfig;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;

/**
 * @author Zhaopo Liu
 * @date 2020/7/20 13:52
 */
@Slf4j
public class MyHiveUtil {

    protected Connection conn;
    protected Statement stmt;
    protected ResultSet rs;

    private HiveConnectionConfig hiveConnectionConfig;

    /**
     * 通过构造器，加载驱动、创建连接
     *
     * @param hiveConnectionConfig
     */
    public MyHiveUtil(HiveConnectionConfig hiveConnectionConfig) {
        this.hiveConnectionConfig = hiveConnectionConfig;
        conn = this.hiveConnectionConfig.createFactory();
        try {
            stmt = conn.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
            log.error("createStatement failed!", e);
        }
    }

    /**
     * 创建数据库
     *
     * @param sql
     * @throws Exception
     */
    public void createDatabase(String sql) throws Exception {
//        String sql = "create database hive_jdbc_test";
        log.info("Running: " + sql);
        stmt.execute(sql);
        closeDB();
    }

    /**
     * 查询所有数据库
     *
     * @param sql
     * @throws Exception
     */
    public void showDatabases(String sql) throws Exception {
//        String sql = "show databases";
        log.info("Running: " + sql);
        rs = stmt.executeQuery(sql);
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
        closeDB();
    }

    /**
     * 创建表
     *
     * @param sql
     * @throws Exception
     */
    public void createTable(String sql) throws Exception {
//        String sql = "create table emp(\n" +
//                "empno int,\n" +
//                "ename string,\n" +
//                "job string,\n" +
//                "mgr int,\n" +
//                "hiredate string,\n" +
//                "sal double,\n" +
//                "comm double,\n" +
//                "deptno int\n" +
//                ")\n" +
//                "row format delimited fields terminated by '\\t'";
        log.info("Running: " + sql);
        stmt.execute(sql);
        closeDB();
    }

    /**
     * 查询所有表
     *
     * @param sql
     * @throws Exception
     */
    public void showTables(String sql) throws Exception {
//        String sql = "show tables";
        log.info("Running: " + sql);
        rs = stmt.executeQuery(sql);
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
        closeDB();
    }

    /**
     * 查看表结构
     *
     * @param sql
     * @throws Exception
     */
    public void descTable(String sql) throws Exception {
//        String sql = "desc emp";
        log.info("Running: " + sql);
        rs = stmt.executeQuery(sql);
        while (rs.next()) {
            System.out.println(rs.getString(1) + "\t" + rs.getString(2));
        }
        closeDB();
    }

    /**
     * 加载数据
     *
     * @param filePath
     * @param sql
     * @throws Exception
     */
    public void loadData(String filePath, String sql) throws Exception {
//        String filePath = "/home/hadoop/data/emp.txt";
//        String sql = "load data local inpath '" + filePath + "' overwrite into table emp";
        log.info("Running: " + sql);
        stmt.execute(sql);
        closeDB();
    }

    /**
     * 查询数据
     *
     * @param sql
     * @throws Exception
     */
    public void selectData(String sql) throws Exception {
//        String sql = "select * from emp";
        log.info("Running: " + sql);
        rs = stmt.executeQuery(sql);
        System.out.println("员工编号" + "\t" + "员工姓名" + "\t" + "工作岗位");
        while (rs.next()) {
            System.out.println(rs.getString("empno") + "\t\t" + rs.getString("ename") + "\t\t" + rs.getString("job"));
        }
        closeDB();
    }

    /**
     * 统计查询（会运行mapreduce作业）
     *
     * @param sql
     * @throws Exception
     */
    public void countData(String sql) throws Exception {
//        String sql = "select count(1) from emp";
        log.info("Running: " + sql);
        rs = stmt.executeQuery(sql);
        while (rs.next()) {
            System.out.println(rs.getInt(1) );
        }
        closeDB();
    }

    /**
     * 删除数据库
     *
     * @param sql
     * @throws Exception
     */
    public void dropDatabase(String sql) throws Exception {
//        String sql = "drop database if exists hive_jdbc_test";
        log.info("Running: " + sql);
        stmt.execute(sql);
        closeDB();
    }

    /**
     * 删除数据库表
     *
     * @param sql
     * @throws Exception
     */
    public void deopTable(String sql) throws Exception {
//        String sql = "drop table if exists emp";
        log.info("Running: " + sql);
        stmt.execute(sql);
        closeDB();
    }

    /**
     * 释放资源
     *
     * @throws Exception
     */
    public void closeDB() throws Exception {
        if ( rs != null) {
            rs.close();
        }
        if (stmt != null) {
            stmt.close();
        }
        if (conn != null) {
            conn.close();
        }
    }

}
