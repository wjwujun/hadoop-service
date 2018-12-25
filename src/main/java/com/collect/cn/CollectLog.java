package com.collect.cn;


import java.util.Timer;

public class CollectLog {

    public static void main(String[] args) {
        Timer timer = new Timer();

        /*定时器任务*/
        //采集任务
        timer.schedule( new CollectTask(),0,60*60*1000L);

        //定时清理任务
        timer.schedule(new BackCleanTask(),0,60*60*1000L);




    }

}
