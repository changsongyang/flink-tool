package com.lzhpo.flinkhadoop.sink;

import com.lzhpo.flinkhadoop.config.HadoopConnectionConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Hadoop Sink
 *
 * @author Zhaopo Liu
 * @date 2020/6/20 03:14
 */
@Slf4j
public class HadoopSink<IN> extends RichSinkFunction<IN> {

    /** 序列化 */
    private SerializationSchema<IN> schema;

    /** Hadoop连接工具 */
    private HadoopConnectionConfig hadoopConnectionConfig;

    /** 写入到HDFS的文件以及路径 */
    private String fileWithPath;

    /** 如果文件存在，是否覆盖 */
    private boolean overwrite = true;

    /** 使用缓存的大小 */
    private Integer cacheSize = 4096;

    /** fileSystem，用完要关闭 */
    protected FileSystem fileSystem;

    public HadoopSink(SerializationSchema<IN> serializationSchema,
                      HadoopConnectionConfig hadoopConnectionConfig,
                      String fileWithPath) {
        this.schema = serializationSchema;
        this.hadoopConnectionConfig = hadoopConnectionConfig;
        this.fileWithPath = fileWithPath;
    }

    public HadoopSink(SerializationSchema<IN> serializationSchema,
                      HadoopConnectionConfig hadoopConnectionConfig,
                      String fileWithPath,
                      boolean overwrite,
                      Integer cacheSize) {
        this.schema = serializationSchema;
        this.hadoopConnectionConfig = hadoopConnectionConfig;
        this.fileWithPath = fileWithPath;
        this.overwrite = overwrite;
        this.cacheSize = cacheSize;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        fileSystem = hadoopConnectionConfig.createFactory();
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
    public void invoke(IN value, Context context) throws Exception {
        FSDataOutputStream fsDataOutputStream = fileSystem.create(
                new Path(fileWithPath), overwrite, cacheSize);
        // 数据写入到hdfs
        fsDataOutputStream.write(schema.serialize(value));
        // 强制将缓冲区的内容刷出
        fsDataOutputStream.flush();
    }
}
