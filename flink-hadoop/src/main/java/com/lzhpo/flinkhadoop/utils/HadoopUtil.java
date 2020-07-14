package com.lzhpo.flinkhadoop.utils;

import com.lzhpo.flinkhadoop.config.HadoopConnectionConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.*;

/**
 * 封装的HDFS操作工具
 *
 * @author lzhpo
 */
@Slf4j
public class HadoopUtil {
    private HadoopConnectionConfig hadoopConnectionConfig;

    protected FileSystem fileSystem;

    public HadoopUtil(HadoopConnectionConfig hadoopConnectionConfig) {
        this.hadoopConnectionConfig = hadoopConnectionConfig;
    }

    /**
     * 创建目录
     * <p>
     * 支持递归创建目录
     *
     * @param filePath 目录
     * @throws Exception Exception
     * @return boolean 成功与否
     */
    public boolean mkDir(String filePath) throws Exception {
        try {
            return fileSystem.mkdirs(new Path(filePath));
        } catch (IOException | IllegalArgumentException e) {
            log.error("创建目录 {} 失败！", filePath, e);
            return false;
        }
    }

    /**
     * 创建指定权限的目录
     * <p>
     * FsPermission(FsAction u, FsAction g, FsAction o) 的三个参数分别对应：
     *      创建者权限，同组其他用户权限，其他用户权限，权限值定义在 FsAction 枚举类中。
     *
     * @param filePath
     * @param u
     * @param g
     * @param o
     * @throws Exception
     * @return boolean 成功与否
     */
    public boolean mkDirWithPermission(String filePath,
                                    FsAction u, FsAction g, FsAction o) throws Exception {
//        fileSystem.mkdirs(new Path("/hdfs-api/test1/"), new FsPermission(FsAction.READ_WRITE, FsAction.READ, FsAction.READ));
        try {
            return fileSystem.mkdirs(new Path(filePath), new FsPermission(u, g, o));
        } catch (IOException | IllegalArgumentException e) {
            log.error("创建目录 {} 失败！", filePath, e);
            return false;
        }
    }

    /**
     * 创建文件，并写入内容
     *
     * @throws Exception
     * @return boolean 成功与否
     */
    public boolean create(String fileWithPath, byte[] dataBytes, boolean overwrite, Integer bufferSize) throws Exception {
        // 如果文件存在，默认会覆盖, 可以通过第二个参数进行控制。第三个参数可以控制使用缓冲区的大小
        try {
            FSDataOutputStream out = fileSystem.create(new Path(fileWithPath), overwrite, bufferSize);
            out.write(dataBytes);
            // 强制将缓冲区中内容刷出
            out.flush();
            out.close();
            return true;
        } catch (IOException | IllegalArgumentException e) {
            log.error("创建文件 {} 写入内容失败！", fileWithPath, e);
            return false;
        }
    }

    /**
     * 判断文件是否存在
     *
     * @param fileWithPath
     * @throws Exception
     * @return boolean 是否存在
     */
    public boolean exist(String fileWithPath) throws Exception {
        return fileSystem.exists(new Path(fileWithPath));
    }

    /**
     * 查看文件内容
     *
     * @param fileWithPath
     * @param encode 编码，例如：UTF-8
     * @throws Exception
     * @return boolean 是否存在
     */
    public String readToString(String fileWithPath, String encode) throws Exception {
        FSDataInputStream inputStream = fileSystem.open(new Path(fileWithPath));
        String content = inputStreamToString(inputStream, encode);
        log.info("文件内容：{}", content);
        return content;
    }

    /**
     * 文件重命名
     *
     * @param oldFileWithPath 旧文件名称以及路径
     * @param newFileWithPath 新文件名称以及路径
     * @throws Exception Exception
     * @return boolean 结果
     */
    public boolean rename(String oldFileWithPath, String newFileWithPath) throws Exception {
        Path oldPath = new Path(oldFileWithPath);
        Path newPath = new Path(newFileWithPath);
        return fileSystem.rename(oldPath, newPath);
    }

    /**
     * 删除目录或文件
     *
     * @param fileWithPath 要删除的文件以及路径
     * @param recursiveDelete 是否递归删除
     * @throws Exception Exception
     * @return boolean 结果
     */
    public boolean delete(String fileWithPath, boolean recursiveDelete) throws Exception {
        /*
         *  第二个参数代表是否递归删除
         *    +  如果 path 是一个目录且递归删除为 true, 则删除该目录及其中所有文件;
         *    +  如果 path 是一个目录但递归删除为 false,则会则抛出异常。
         */
        return fileSystem.delete(new Path(fileWithPath), recursiveDelete);
    }

    /**
     * 上传文件到HDFS
     *
     * @param localFileWithPath 需要上传的文件路径
     * @param hdfsFileWithPath 上传到HDFS的文件路径
     * @throws Exception Exception
     * @return boolean 结果
     */
    public boolean copyFromLocalFile(String localFileWithPath, String hdfsFileWithPath) throws Exception {
        try {
            // 如果指定的是目录，则会把目录及其中的文件都复制到指定目录下
            Path src = new Path(localFileWithPath);
            Path dst = new Path(hdfsFileWithPath);
            fileSystem.copyFromLocalFile(src, dst);
            return true;
        } catch (IllegalArgumentException | IOException e) {
            return false;
        }
    }

    /**
     * 上传大文件并显示上传进度
     *
     * @param localFileWithPath 需要上传的文件路径
     * @param hdfsFileWithPath 上传到HDFS的文件路径
     * @param bufferSize 缓冲大小 例如：4096
     * @throws Exception Exception
     * @return boolean 结果
     */
    public boolean copyFromLocalBigFile(String localFileWithPath, String hdfsFileWithPath, Integer bufferSize) throws Exception {

        try {
            File file = new File(localFileWithPath);
            final float fileSize = file.length();
            InputStream in = new BufferedInputStream(new FileInputStream(file));

            FSDataOutputStream out = fileSystem.create(new Path(hdfsFileWithPath),
                    new Progressable() {
                        long fileCount = 0;

                        @Override
                        public void progress() {
                            fileCount++;
                            // progress 方法每上传大约 64KB 的数据后就会被调用一次
                            System.out.println("上传进度：" + (fileCount * 64 * 1024 / fileSize) * 100 + " %");
                        }
                    });

            IOUtils.copyBytes(in, out, bufferSize);

            return true;
        } catch (IOException | IllegalArgumentException e) {
            e.printStackTrace();
            log.error("上传大文件 {} 到HDFS失败！", localFileWithPath, e);
            return false;
        }

    }

    /**
     * 从HDFS上下载文件
     *
     * @param hdfsFileWithPath hdfs上的文件路径
     * @param downLoadToLocalPath 下载到本地的文件路径
     * @throws Exception Exception
     * @return boolean 结果
     */
    public boolean copyToLocalFile(String hdfsFileWithPath, String downLoadToLocalPath) throws Exception {
        try {
            Path src = new Path(hdfsFileWithPath);
            Path dst = new Path(downLoadToLocalPath);
            /*
             * 第一个参数控制下载完成后是否删除源文件,默认是 true,即删除;
             * 最后一个参数表示是否将 RawLocalFileSystem 用作本地文件系统;
             * RawLocalFileSystem 默认为 false,通常情况下可以不设置,
             * 但如果你在执行时候抛出 NullPointerException 异常,则代表你的文件系统与程序可能存在不兼容的情况 (window 下常见),
             * 此时可以将 RawLocalFileSystem 设置为 true
             */
            fileSystem.copyToLocalFile(false, src, dst, true);
            return true;
        } catch (IllegalArgumentException | IOException e) {
            log.error("从HDFS上读取文件 {} 失败！", hdfsFileWithPath, e);
            return false;
        }
    }

    /**
     * 查看指定目录下所有文件的信息
     *
     * @param hdfsFilePath hdfs上的文件目录
     * @throws Exception Exception
     * @return String 结果
     */
    public String listFiles(String hdfsFilePath) throws Exception {
        FileStatus[] statuses = fileSystem.listStatus(new Path(hdfsFilePath));
        for (FileStatus fileStatus : statuses) {
            //fileStatus 的 toString 方法被重写过，直接打印可以看到所有信息
            System.out.println(fileStatus.toString());
        }
        return statuses.toString();
    }

    /**
     * 递归查看指定目录下所有文件的信息
     *
     * @param hdfsFilePath hdfs上的文件目录
     * @param recursiveDelete 是否递归查看
     * @throws Exception Exception
     * @return String 结果
     */
    public String listFilesRecursive(String hdfsFilePath, boolean recursiveDelete) throws Exception {
        RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(new Path(hdfsFilePath), recursiveDelete);
        while (files.hasNext()) {
            System.out.println(files.next());
        }
        return files.toString();
    }

    /**
     * 查看文件的块信息
     *
     * @param hdfsFileWithPath hdfs上的文件以及路径
     * @throws Exception Exception
     * @return String 结果
     */
    public String getFileBlockLocations(String hdfsFileWithPath) throws Exception {
        FileStatus fileStatus = fileSystem.getFileStatus(new Path(hdfsFileWithPath));
        BlockLocation[] blocks = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
        for (BlockLocation block : blocks) {
            System.out.println(block);
        }
        return blocks.toString();
    }

    /**
     * 把输入流转换为指定编码的字符
     *
     * @param inputStream 输入流
     * @param encode      指定编码类型
     */
    private String inputStreamToString(InputStream inputStream, String encode) {
        try {
            if (encode == null || ("".equals(encode))) {
                encode = "utf-8";
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
