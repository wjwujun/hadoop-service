package com.hsfs;

import com.hsfs.common.myContext;
import com.hsfs.dao.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class HdfsWordcount {

    public static void main(String[] args) throws Exception {

        /*初始化配置文件*/
        Properties prop = new Properties();
        prop.load(HdfsWordcount.class.getClassLoader().getResourceAsStream("job.properties"));

        /*反射*/
        Class<?> mapper_class = Class.forName(prop.getProperty("MAPPER_CLASS"));
        Mapper mapper = (Mapper) mapper_class.newInstance();

        /*结果缓存*/
        myContext myContext = new myContext();



        //去hdfs中读取
        FileSystem fs = FileSystem.get(new URI("hdfs://122.225.58.117:9000/"), new Configuration(), "root");
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path("/aa/"), false);

        while (iterator.hasNext()){
            LocatedFileStatus file = iterator.next();
            FSDataInputStream in = fs.open(file.getPath());
            //逐行读取
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String line=null;
            while ((line=br.readLine())!=null){
                //调用一个方法，对每一行业务进行处理
                mapper.map(line,myContext);

            }
            br.close();
            in.close();
        }


        /*输出结果*/
        HashMap<Object, Object> contextMap = myContext.getContextMap();

        Path outPath = new Path("/out/");
        if(!fs.exists(outPath)){
            fs.mkdirs(outPath);
        }

        FSDataOutputStream out = fs.create(new Path("/res.dat"));

        Set<Map.Entry<Object, Object>> entries = contextMap.entrySet();
        for (Map.Entry<Object, Object> entry : entries) {
            out.write((entry.getKey().toString()+"\t"+entry.getValue()+"\n").getBytes());
        }
        out.close();
        fs.close();

        System.out.println("数据统计完成");

    }








}





