package com.collect.cn;

import java.io.File;
import java.io.FilenameFilter;
import java.util.TimerTask;

public class CollectTask extends TimerTask {

    @Override
    public void run() {
        /*
        * 定时探测日志源目录
        * 获取需要采集的文件
        * 移动这些文件到一个待上传的临时目录
        * 遍历待上传目录中的文件，逐一传输到hdfs的目标路径，同时将刚传输完成的目录移动到备份目录
        * */

         File srcDir = new File("E:/javaproject/log");

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

         //将要采集的文件移动到待上传临时目录
        for (File file : listFiles) {
                file.renameTo(new File("E:/javaproject/log_temp"));
        }

        //构造一个hdfs的客户端对象


    }


























}
