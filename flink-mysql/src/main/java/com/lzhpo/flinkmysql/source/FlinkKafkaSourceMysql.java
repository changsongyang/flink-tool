package com.lzhpo.flinkmysql.source;

import com.lzhpo.flinkmysql.config.MysqlConnectionConfig;
import com.lzhpo.flinkmysql.test.User;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * 从MySQL中读取数据
 *
 * @author lzhpo
 */
public class FlinkKafkaSourceMysql<T> extends RichSourceFunction<User> {

    private PreparedStatement ps;
    private Connection connection;

    private DeserializationSchema<T> deserializationSchema;
    private MysqlConnectionConfig mysqlConnectionConfig;

    protected boolean running = true;

  public FlinkKafkaSourceMysql(DeserializationSchema<T> deserializationSchema,
                               MysqlConnectionConfig mysqlConnectionConfig) {
    this.deserializationSchema = deserializationSchema;
    this.mysqlConnectionConfig = mysqlConnectionConfig;
  }

  @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = mysqlConnectionConfig.createFactory();
        // 执行sql的语句
        ps = this.connection.prepareStatement("SELECT id,name,location FROM `study-flink`.`tb_user`;");
    }

    @Override
    public void run(SourceContext<User> ctx) throws Exception {
        while (running) {
            ResultSet resultSet = ps.executeQuery();
            //处理结果集
            while (resultSet.next()) {
                User user = User.builder()
                        .setId(resultSet.getLong("id"))
                        .setName(resultSet.getString("name").trim())
                        .setLocation(resultSet.getString("location").trim())
                        .build();
                // 发送结果
                ctx.collect(user);
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
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
}
