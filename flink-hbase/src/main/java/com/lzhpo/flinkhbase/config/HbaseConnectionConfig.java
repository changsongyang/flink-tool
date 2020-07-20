package com.lzhpo.flinkhbase.config;

import com.lzhpo.common.BaseFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * @author Zhaopo Liu
 * @date 2020/6/20 03:14
 */
@Slf4j
public class HbaseConnectionConfig extends BaseFactory<Connection> {

    /** zk连接IP地址/主机名称，集群使用逗号隔开 */
    private String zkQuorum = "hadoop";
    /** zk的端口号 */
    private int zkPort = 2181;

    protected Connection connection;

    @Override
    public Connection createFactory() {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", String.valueOf(zkPort));
        configuration.set("hbase.zookeeper.quorum", zkQuorum);
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            log.error("connect hbase [{}] failed.", zkQuorum +":" +zkPort, e);
        }
        return connection;
    }

    /**
     * This for {@link HbaseConnectionConfig} build
     *
     * @return Builder
     */
    public static Builder builder() {
        return new Builder();
    }

    private static class Builder {
        private HbaseConnectionConfig hbaseConnectionConfig;

        public Builder setZkQuorum(String zkQuorum) {
            hbaseConnectionConfig.zkQuorum = zkQuorum;
            return this;
        }

        public Builder setZkPort(int zkPort) {
            hbaseConnectionConfig.zkPort = zkPort;
            return this;
        }

        public HbaseConnectionConfig build() {
            return hbaseConnectionConfig;
        }
    }
}
