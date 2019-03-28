package top;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/***
 *取出访问量前几名的网页
 * @author wj
 * @param
 */
public class jobSubmitter {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf=new Configuration();



        conf.setInt("top.n", 10);        //通过代码设置要取值的范围
        //conf.setInt("top.n", Integer.parseInt(args[0]));  //通过man函数传参

        Job job=Job.getInstance(conf);


        job.setJarByClass(jobSubmitter.class);
        job.setMapperClass(pageTopMapper.class);
        job.setReducerClass(pageTopReducer.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job,new Path("E:/data/input"));
        FileOutputFormat.setOutputPath(job,new Path("E:/data/out/top"));

        job.waitForCompletion(true);

    }


}



















