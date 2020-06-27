package com.lzhpo.flinkmysql.source;

import com.lzhpo.flinkmysql.MysqlConfigConstant;
import com.lzhpo.flinkmysql.MysqlFactory;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Preconditions;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author lzhpo
 */
public class FlinkMysqlSource<T> extends RichSourceFunction<ArrayList> {

    PreparedStatement ps = null;
    Connection connection = null;

    DeserializationSchema<T> deserializationSchema;

    public FlinkMysqlSource(DeserializationSchema<T> deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = new MysqlFactory().createMysqlFactory();
        // 执行sql的语句
        ps = this.connection.prepareStatement(MysqlConfigConstant.SQL);
    }

    @Override
    public void run(SourceContext<ArrayList> ctx) throws Exception {
        ResultSet resultSet = ps.executeQuery();
        ArrayList<Object> arrayList = new ArrayList<>();
        // 获取键名
        ResultSetMetaData metaData = resultSet.getMetaData();
        // 获取行的数量
        int columnCount = metaData.getColumnCount();
        while (resultSet.next()) {
            HashMap<Object, Object> rowData = new HashMap<>();
            for (int i = 1; i <= columnCount; i++) {
                // 获取键名以及键值
                rowData.put(metaData.getColumnName(i), resultSet.getObject(i));
            }
            arrayList.add(rowData);
            // 返回ArrayList
            ctx.collect(arrayList);
        }
    }

    @Override
    public void cancel() {
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }

    /**
     * build
     *
     * @param <T>
     * @return
     */
    public static <T> FlinkMysqlSource<T> build(DeserializationSchema<T> deserializationSchema) {
        Preconditions.checkNotNull(deserializationSchema, "deserializationSchema cannot be null");
        return new FlinkMysqlSource<>(deserializationSchema);
    }

    public FlinkMysqlSource<T> setUrl(String url) {
        MysqlConfigConstant.URL = url;
        return this;
    }

    public FlinkMysqlSource<T> setUsername(String username) {
        MysqlConfigConstant.USERNAME = username;
        return this;
    }

    public FlinkMysqlSource<T> setPassword(String password) {
        MysqlConfigConstant.PASSWORD = password;
        return this;
    }

    public FlinkMysqlSource<T> setSql(String sql) {
        MysqlConfigConstant.SQL = sql;
        return this;
    }

}
