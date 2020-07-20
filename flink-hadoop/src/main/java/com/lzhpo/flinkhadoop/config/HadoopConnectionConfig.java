package com.lzhpo.flinkhadoop.config;

import com.lzhpo.common.BaseFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Hadoop连接工厂
 *
 * @author Zhaopo Liu
 * @date 2020/6/20 03:14
 */
public class HadoopConnectionConfig extends BaseFactory<FileSystem> {

    private String hadoopUrl = "localhost:9000";
    private String user = "root";

    @Override
    public FileSystem createFactory() {
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "1");
        try {
            return FileSystem.get(new URI(hadoopUrl), configuration, user);
        } catch (IOException | InterruptedException | URISyntaxException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Builder for {@link HadoopConnectionConfig}
     * @return
     */
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private HadoopConnectionConfig hadoopConnectionConfig;

        public Builder setHadoopUrl(String hadoopUrl) {
            hadoopConnectionConfig.hadoopUrl = hadoopUrl;
            return this;
        }

        public Builder setUser(String user) {
            hadoopConnectionConfig.user = user;
            return this;
        }

        public HadoopConnectionConfig build() {
            return hadoopConnectionConfig;
        }
    }
}
