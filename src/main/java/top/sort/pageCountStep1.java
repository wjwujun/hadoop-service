package top.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/***
 * 全局排序：
 *   第一步：将所有页面的访问量统计出来
 * @author wj
 * @param
 */
public class pageCountStep1 {

    /*mapper类*/
    public static class pageCountStep1Mapper extends Mapper<LongWritable, Text,Text, IntWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] lines = line.split(" ");

            context.write(new Text(lines[1]),new IntWritable(1));
        }
    }

    /*reduce类*/
    public static class pageCountStep1Reduce extends Reducer<Text,IntWritable,Text,IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
                int count=0;
            for (IntWritable value : values) {
                count+=value.get();
            }
            context.write(new Text(key),new IntWritable(count));
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf=new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(pageCountStep1.class);
        job.setMapperClass(pageCountStep1Mapper.class);
        job.setReducerClass(pageCountStep1Reduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        FileInputFormat.setInputPaths(job,new Path("E:/data/input/request"));
        FileOutputFormat.setOutputPath(job,new Path("E:/data/out/top"));

        job.waitForCompletion(true);


    }

}
