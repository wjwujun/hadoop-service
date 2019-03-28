package index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/***
 * 数据切片:
 *  第二步：获取c++-c.txt	2   类型数据
 * @author wj
 * @param
 */
public class indexStepTwo {


    public static  class IndexStepTwoMapper extends Mapper<LongWritable, Text,Text, Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String lines = value.toString();
            String[] words = lines.split("-");
            context.write(new Text(words[0]),new Text(words[1].replaceAll("\t","-->")));
        }
    }

    public static  class IndexStepTwoReduce extends Reducer<Text,Text,Text,Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            StringBuilder str = new StringBuilder();
            // stringbuffer是线程安全的，stringbuilder是非线程安全的，在不涉及线程安全的场景下，stringbuilder更快
            for (Text value : values) {
                    str.append(value.toString()).append("\t");
            }
            //获取 kitty	a.txt-->1	b.txt-->1 格式数据
            context.write(key,new Text(str.toString()));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf=new Configuration();
        Job job=Job.getInstance(conf);


        job.setJarByClass(indexStepTwo.class);
        job.setMapperClass(IndexStepTwoMapper.class);
        job.setReducerClass(IndexStepTwoReduce.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job,new Path("E:/data/out/indexOne"));
        FileOutputFormat.setOutputPath(job,new Path("E:/data/out/indexTwo"));

        job.waitForCompletion(true);
    }

}
