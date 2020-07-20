package com.lzhpo.flinkhadoop.source;

import com.lzhpo.flinkhadoop.config.HadoopConnectionConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Objects;

/**
 * Hadoop Source
 *
 * @author Zhaopo Liu
 * @date 2020/6/20 03:14
 */
@Slf4j
public class HadoopSource<OUT> extends RichSourceFunction<OUT> {

    /** 反序列化 */
    private DeserializationSchema<OUT> schema;

    /** Hadoop连接工具 */
    private HadoopConnectionConfig hadoopConnectionConfig;

    /** 查看的文件路径 */
    private String fileWithPath;

    /** fileSystem，用完要关闭 */
    protected FileSystem fileSystem;

    /** 控制程序运行 */
    protected boolean running = true;

    public HadoopSource(DeserializationSchema<OUT> deserializationSchema,
                        HadoopConnectionConfig hadoopConnectionConfig,
                        String fileWithPath) {
        this.schema = deserializationSchema;
        this.hadoopConnectionConfig = hadoopConnectionConfig;
        this.fileWithPath = fileWithPath;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        fileSystem = hadoopConnectionConfig.createFactory();
    }

    @Override
    public void run(SourceContext<OUT> ctx) throws Exception {
        while (running) {
            FSDataInputStream fsDataInputStream = fileSystem.open(new Path(fileWithPath));
            String readData = inputStreamToString(fsDataInputStream, "UTF-8");
            log.info("readData:{}",readData);
            ctx.collect(schema.deserialize(Objects.requireNonNull(readData).getBytes()));
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (fileSystem != null) {
            fileSystem.close();
            log.info("Already close hdfs connection.");
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    /**
     * 把输入流转换为指定编码的字符
     *
     * @param inputStream 输入流
     * @param encode      指定编码类型
     */
    private static String inputStreamToString(InputStream inputStream, String encode) {
        try {
            if (encode == null || ("".equals(encode))) {
                encode = "UTF-8";
            }
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, encode));
            StringBuilder builder = new StringBuilder();
            String str = "";
            while ((str = reader.readLine()) != null) {
                builder.append(str).append("\n");
            }
            return builder.toString();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

}
