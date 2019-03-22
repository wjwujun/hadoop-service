package mapper.reducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;


/*
* 用于提交mapreduce job客户端程序
* 功能：
*   1、封装本次job运行时所必须的参数
*   2、跟yarn进行交互，将mapreduce程序成功的启动，运行
*
* */
public class JobSubmitter {

    public static void main(String[] args) throws Exception{
        // 在代码中设置JVM系统参数，用于给job对象来获取访问HDFS的用户身份
        System.setProperty("HADOOP_USER_NAME", "root");


        Configuration conf = new Configuration();

        // 1、设置job运行时要访问的默认文件系统
        conf.set("fs.defaultFS", "hdfs://localhost:9000");    //设置hdfs的通讯地址，和job运行时要访问的文件系统
        // 2、设置job提交到哪去运行
        conf.set("mapreduce.framework.name", "local");
        conf.set("yarn.resourcemanager.hostname", "localhost");
        // 3、如果要从windows系统上运行这个job提交客户端程序，则需要加这个跨平台提交的参数
        //conf.set("mapreduce.app-submission.cross-platform","true");

        Job job = Job.getInstance(conf);

        // 1、封装参数：jar包所在的位置
        job.setJarByClass(JobSubmitter.class);  //根据类加载的位置，判断jar在哪个位置。



        // 2、封装参数： 本次job所要调用的Mapper实现类、Reducer实现类
        job.setMapperClass(WordcountMapper.class);
        job.setReducerClass(WordcountReducer.class);

        // 3、封装参数：本次job的Mapper实现类、Reducer实现类产生的结果数据的key、value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        //4、本次job输入数据集要处理的路径,最终结果的输出路径
        FileInputFormat.addInputPath(job, new Path("file:/E:/a.txt"));     //读入文件路径


        FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:9000/"),conf,"root"); //以root用户运行
        Path outPath = new Path("/out");
        if(fs.exists(outPath)){
            fs.delete(outPath,true);
        }
        FileOutputFormat.setOutputPath(job, outPath); //输出路径必须不存在

        // 5、封装参数：想要启动的reduce task的数量
        job.setNumReduceTasks(1);   //默认值就是1个

        // 6、提交job给yarn
        //job.submit();  提交完就结束，不知道运行结果
        boolean result = job.waitForCompletion(true);//阻塞，等待结果完成
        if (result) {
            System.out.println("job任务执行成功");
        }

    }


}
