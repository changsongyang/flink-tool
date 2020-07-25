package com.lzhpo.common.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileOutputStream;

/**
 * 数据持久化到磁盘工具类
 *
 * @author Zhaopo Liu
 * @date 2020/7/25 18:17
 */
@Slf4j
public class SaveDataToDiskUtil {

    /**
     * 数据持久化到磁盘路径
     *
     * @param data 数据字符串
     * @param savePath 保存到磁盘路径
     * @return {boolean}
     */
    public static boolean saveStringToDisk(String data, String savePath) {
        if (StringUtils.isNotEmpty(data)) {
            byte[] sourceByte = data.getBytes();
            try {
                // 文件路径（路径+文件名）
                File file = new File(savePath);
                // 文件不存在则创建文件，先创建目录
                if (!file.exists()) {
                    File dir = new File(file.getParent());
                    dir.mkdirs();
                    file.createNewFile();
                }
                // 文件输出流用于将数据写入文件
                FileOutputStream outStream = new FileOutputStream(file);
                outStream.write(sourceByte);
                // 关闭文件输出流
                outStream.close();
                log.error("数据 {} 持久化到磁盘路径 {} 成功.", data, savePath);
                return true;
            } catch (Exception e) {
                log.error("数据 {} 持久化到磁盘路径 {} 失败.", data, savePath, e);
            }
        }
        return false;
    }

}
