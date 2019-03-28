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
 *  第一步：获取c++-c.txt	2   类型数据
 * @author wj
 * @param
 */
public class indexStepOne {

    public static  class IndexStepOneMapper extends Mapper<LongWritable, Text,Text, IntWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //产生<hello-文件名，1>
            // 从输入切片信息中获取当前正在处理的一行数据所属的文件
            FileSplit file = (FileSplit) context.getInputSplit();
            String name = file.getPath().getName();
            String lines = value.toString();
            String[] words = lines.split(" ");
            for (String word : words) {
                    context.write(new Text(word+"-"+name),new IntWritable(1));
            }
        }
    }

    public static  class IndexStepOneReduce extends Reducer<Text,IntWritable,Text,IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count=0;
            for (IntWritable value : values) {
                count+=value.get();
            }
            context.write(key,new IntWritable(count));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf=new Configuration();
        Job job=Job.getInstance(conf);


        job.setJarByClass(indexStepOne.class);
        job.setMapperClass(IndexStepOneMapper.class);
        job.setReducerClass(IndexStepOneReduce.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job,new Path("E:/data/input/index"));
        FileOutputFormat.setOutputPath(job,new Path("E:/data/out/indexOne"));

        job.waitForCompletion(true);
    }

}
