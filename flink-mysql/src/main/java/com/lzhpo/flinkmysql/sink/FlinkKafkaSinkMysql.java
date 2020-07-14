package com.lzhpo.flinkmysql.sink;

import com.lzhpo.flinkmysql.config.MysqlConnectionConfig;
import com.lzhpo.flinkmysql.test.User;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 * Flink MySQL Sink
 *
 * @author lzhpo
 */
@Slf4j
public class FlinkKafkaSinkMysql<T> extends RichSinkFunction<User> {

    protected PreparedStatement ps;
    protected Connection conn;

    private DeserializationSchema<T> deserializationSchema;
    private MysqlConnectionConfig mysqlConnectionConfig;

    public FlinkKafkaSinkMysql(DeserializationSchema<T> deserializationSchema,
                               MysqlConnectionConfig mysqlConnectionConfig) {
        this.deserializationSchema = deserializationSchema;
        this.mysqlConnectionConfig = mysqlConnectionConfig;
    }

    /**
     * sql语句和实体类需要自己修改为自己对应的，我这里只是demo。
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        conn = mysqlConnectionConfig.createFactory();
        // INSERT INTO `study-flink`.`tb_user`(`id`, `name`, `location`) VALUES (1, 'Pop Liu', '广州')
        String sql = "INSERT INTO `study-flink`.`tb_user`(`id`, `name`, `location`) VALUES (?, ?, ?);";
        ps = this.conn.prepareStatement(sql);
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
    public void invoke(User user, Context context) throws Exception {
        ps.setLong(1, user.getId());
        ps.setString(2, user.getName());
        ps.setString(3, user.getLocation());
        // 执行sql语句
        ps.executeUpdate();
        log.info("insert {} to mysql.", user);
    }
}
