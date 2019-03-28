package top.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/***
 * 全局排序:
 *      第二步：将第一步输出的统计好的每个页面的访问数据进行排序
 * @author wj
 * @param
 */
public class pageCountStep2 {

    /*
    *mapper
    *处理的结果是第一步统计的结果数据
    * */
    public static class PageCountStep2Mapper extends Mapper<LongWritable, Text, PageCount, NullWritable> {

        @Override
        protected void map(LongWritable key, Text value,
                           Mapper<LongWritable, Text, PageCount, NullWritable>.Context context)
                throws IOException, InterruptedException {

            String[] split = value.toString().split("\t");

            PageCount pageCount = new PageCount(split[0], Integer.parseInt(split[1]));
            context.write(pageCount, NullWritable.get());
        }

    }


    public static class PageCountStep2Reducer extends Reducer<PageCount, NullWritable, PageCount, NullWritable> {


        @Override
        protected void reduce(PageCount key, Iterable<NullWritable> values,
                              Reducer<PageCount, NullWritable, PageCount, NullWritable>.Context context)
                throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }


    }


    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(pageCountStep2.class);

        job.setMapperClass(PageCountStep2Mapper.class);
        job.setReducerClass(PageCountStep2Reducer.class);

        job.setMapOutputKeyClass(PageCount.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(PageCount.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job,new Path("E:/data/out/top"));
        FileOutputFormat.setOutputPath(job,new Path("E:/data/out/sort"));

        job.setNumReduceTasks(1);

        job.waitForCompletion(true);

    }


}