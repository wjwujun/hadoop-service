package com.collect.cn;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FilenameFilter;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Logger;

public class CollectTask extends TimerTask {

    @Override
    public void run() {
        //构造一个log4j日志对象
        Logger logger = Logger.getLogger("logRollingFile");
        try {


            /*获取配置文件*/
            Properties props = PropertyHolder.getProps();


            /*
            * 定时探测日志源目录
            * 获取需要采集的文件
            * 移动这些文件到一个待上传的临时目录
            * 遍历待上传目录中的文件，逐一传输到hdfs的目标路径，同时将刚传输完成的目录移动到备份目录
            * */

             File srcDir = new File(props.getProperty(Constants.LOG_SOURCE_DIR));

             //列出日志源中需要采集的文件
             File[] listFiles = srcDir.listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    /*定义过滤的逻辑*/
                    if(name.startsWith("access.log.")) {
                        return true;
                    }
                    return false;
                }
            });

            //记录日志
            logger.info("探测到如下文件："+ Arrays.toString(listFiles));



             //将要采集的文件移动到待上传临时目录
            File toUploadDir = new File(props.getProperty(Constants.LOG_TOUPLOAD_DIR));
            for (File file : listFiles) {
                FileUtils.moveToDirectory(file,toUploadDir,true);
            }


            //记录日志
            logger.info("上述文件移动到待上传目录："+ toUploadDir.getAbsolutePath());


            //构造一个hdfs的客户端对象
            FileSystem fs = FileSystem.get(new URI(props.getProperty(Constants.HDFS_URI)), new Configuration(), "root");
            File[] toUploadFiles = toUploadDir.listFiles();

            //获取本次采集的日期
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-hh");
            String day = sdf.format(new Date());

            //检查hdfs中的目录是否存在，不存在 就创建


            Path hdfsDir = new Path(props.getProperty(Constants.HDFS_DEST_BASE_DIR) + day);
            if(!fs.exists(hdfsDir)){
                fs.mkdirs(hdfsDir);
            }

            //检查备份目录是否存在，不存在就创建。
            File localDir = new File(props.getProperty(Constants.LOG_BACKUP_BASE_DIR) + day+"/");
            if(!localDir.exists()){
                localDir.mkdirs();
            }

            for (File file : toUploadFiles) {
                //将文件传输到hdfs并改名

                Path hdfsPath = new Path(hdfsDir + props.getProperty(Constants.HDFS_FILE_PREFIX) + UUID.randomUUID() + props.getProperty(Constants.HDFS_FILE_SUFFIX));
                fs.copyFromLocalFile(new Path(file.getAbsolutePath()),hdfsPath);
                //记录日志
                logger.info("文件传输到HDFS完成"+file.getAbsolutePath()+"--->"+hdfsPath);


                //备份
                FileUtils.moveToDirectory(file,localDir,true);
                //记录日志
                logger.info("文件备份完成"+file.getAbsolutePath()+"--->"+localDir);
            }




        } catch (Exception e) {
            e.printStackTrace();
        }


    }


























}
