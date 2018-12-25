package com.collect.cn;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimerTask;

public class BackCleanTask extends TimerTask {
    @Override
    public void run() {
        //探测备份目录
        File backDir = new File("E:/javaproject/logs/back/");
        File[] listDir = backDir.listFiles();

        //判断备份日期子目录是否已经超过24小时。
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH");
        long now = new Date().getTime();



        for (File file : listDir) {
            try {
                long time = sdf.parse(file.getName()).getTime();
                if(now - time>24*60*60*1000L){

                    FileUtils.deleteDirectory(file);

                }

            }catch (Exception e) {
                e.printStackTrace();
            }


        }



    }
}
