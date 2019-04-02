package top;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URISyntaxException;

/***
 *取出访问量前几名的网页
 * @author wj
 * @param
 */
public class jobSubmitter {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        /*
        * linux下运行
        * */
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://host-01:9000");
        //conf.set("mapreduce.framework.name", "yarn");  //提交到yarn上面运行
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        Job job = Job.getInstance(conf);


        // 1、封装参数：jar包所在的位置
        job.setJarByClass(jobSubmitter.class);

        // 2、封装参数： 本次job所要调用的Mapper实现类、Reducer实现类
        job.setMapperClass(pageTopMapper.class);
        job.setReducerClass(pageTopReducer.class);

        // 3、封装参数：本次job的Mapper实现类、Reducer实现类产生的结果数据的key、value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        // 4、封装参数：本次job要处理的输入数据集所在路径、最终结果的输出路径
        FileInputFormat.setInputPaths(job, new Path("/input"));
        FileOutputFormat.setOutputPath(job, new Path("/out"));

        // 6、提交job给yarn
        job.waitForCompletion(true);
    }


}



















