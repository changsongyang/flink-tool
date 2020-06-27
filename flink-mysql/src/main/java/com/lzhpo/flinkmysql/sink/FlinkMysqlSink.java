package com.lzhpo.flinkmysql.sink;

import com.lzhpo.flinkmysql.MysqlConfigConstant;
import com.lzhpo.flinkmysql.MysqlFactory;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Preconditions;

import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 * Flink MySQL Sink
 *
 * @author lzhpo
 */
public class FlinkMysqlSink<T> extends RichSinkFunction<String> {

    PreparedStatement ps;
    Connection conn;

    DeserializationSchema<T> deserializationSchema;

    public FlinkMysqlSink(DeserializationSchema<T> deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        conn = new MysqlFactory().createMysqlFactory();
        ps = this.conn.prepareStatement(MysqlConfigConstant.SQL);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (conn != null) {
            conn.close();
        }
        if (ps != null) {
            ps.close();
        }
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        // 执行sql语句
        ps.executeUpdate();
    }

    /**
     * build
     *
     * @param <T>
     * @return
     */
    public static <T> FlinkMysqlSink<T> build(DeserializationSchema<T> deserializationSchema) {
        Preconditions.checkNotNull(deserializationSchema, "deserializationSchema cannot be null");
        return new FlinkMysqlSink<>(deserializationSchema);
    }

    public FlinkMysqlSink<T> url(String url) {
        MysqlConfigConstant.URL = url;
        return this;
    }

    public FlinkMysqlSink<T> username(String username) {
        MysqlConfigConstant.USERNAME = username;
        return this;
    }

    public FlinkMysqlSink<T> password(String password) {
        MysqlConfigConstant.PASSWORD = password;
        return this;
    }

    public FlinkMysqlSink<T> sql(String sql) {
        MysqlConfigConstant.SQL = sql;
        return this;
    }
}
