package flow;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class JobSubMitter {

    public static void main(String[] args) throws Exception{

        Configuration  conf=new Configuration();
        Job job=Job.getInstance(conf);

        job.setJarByClass(JobSubMitter.class);

        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);

        //设置参数指定maptask的时候用哪个分区， 如果不指定会使用默认的Hashpartitioner
        job.setPartitionerClass(ProvincePartitioner.class);
        // 由于我们的ProvincePartitioner可能会产生6种分区号，所以，需要有6个reduce task来接收
        job.setNumReduceTasks(6);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        FileInputFormat.setInputPaths(job,new Path("E:/data/input"));
        FileOutputFormat.setOutputPath(job,new Path("E:/data/out"));

        job.waitForCompletion(true);


    }

}
