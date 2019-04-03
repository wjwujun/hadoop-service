package appData;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/***
 * 数据预处理
 * @author wj
 * @date 2019/4/3 0003 15:56
 * @param
 */
public class DataClean {
    public  static class CleanMapper extends Mapper<LongWritable, Text,Text, NullWritable>{
        Text k = null;
        NullWritable v = null;
        SimpleDateFormat sdf = null;
        MultipleOutputs<Text,NullWritable> mos = null;  //多路输出器


        /***
         * @author wj
         * @date 2019/4/3 0003 15:41
         * @param [context]
         * 初始化执行一次
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            k = new Text();
            v = NullWritable.get();
            sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            mos = new MultipleOutputs<Text,NullWritable>(context);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            /*数据源为json*/
            JSONObject jsonObj = JSON.parseObject(value.toString());
            /*获取头部信息*/
            JSONObject headerObj = jsonObj.getJSONObject("header");

            //过滤缺失必选字段的记录
            if (StringUtils.isBlank(headerObj.getString("sdk_ver"))) return;
            if (StringUtils.isBlank(headerObj.getString("time_zone"))) return;
            if (StringUtils.isBlank(headerObj.getString("commit_id")))return;
            if (StringUtils.isBlank(headerObj.getString("commit_time"))) {
                return;
            }else{
                // 追加的逻辑，替换掉原始数据中的时间戳
                String commit_time = headerObj.getString("commit_time");
                String format = sdf.format(new Date(Long.parseLong(commit_time)+38*24*60*60*1000L));
                headerObj.put("commit_time", format);
            }
            if (StringUtils.isBlank(headerObj.getString("pid"))) return;
            if (StringUtils.isBlank(headerObj.getString("app_token"))) return;
            if (StringUtils.isBlank(headerObj.getString("app_id"))) return;
            if (StringUtils.isBlank(headerObj.getString("device_id"))) return;
            if (StringUtils.isBlank(headerObj.getString("device_id_type"))) return;
            if (StringUtils.isBlank(headerObj.getString("release_channel"))) return;
            if (StringUtils.isBlank(headerObj.getString("app_ver_name"))) return;
            if (StringUtils.isBlank(headerObj.getString("app_ver_code")))return;
            if (StringUtils.isBlank(headerObj.getString("os_name"))) return;
            if (StringUtils.isBlank(headerObj.getString("os_ver"))) return;
            if (StringUtils.isBlank(headerObj.getString("language"))) return;
            if (StringUtils.isBlank(headerObj.getString("country"))) return;
            if (StringUtils.isBlank(headerObj.getString("manufacture"))) return;
            if (StringUtils.isBlank(headerObj.getString("device_model")))    return;
            if (StringUtils.isBlank(headerObj.getString("resolution"))) return;
            if (StringUtils.isBlank(headerObj.getString("net_type"))) return;

            /**
             * 生成user_id
             * android_id   android
             *device_id   ios
             */
            String user_id = "";
            if ("android".equals(headerObj.getString("os_name").trim())) {
                user_id = StringUtils.isNotBlank(headerObj.getString("android_id")) ? headerObj.getString("android_id"): headerObj.getString("device_id");
            } else {
                user_id = headerObj.getString("device_id");
            }

            /**
             * 输出结果
             */
            headerObj.put("user_id", user_id);
            k.set(JsonToStringUtil.toString(headerObj));

            if("android".equals(headerObj.getString("os_name"))){
                mos.write(k, v, "android/android");
            }else{
                mos.write(k, v, "ios/ios");
            }


        }

        /***
         * 结束执行一次
         * @author wj
         * @date 2019/4/3 0003 15:41
         * @param [context]
         */
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
                mos.close();
        }

    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(DataClean.class);

        job.setMapperClass(CleanMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(0);

        // 避免生成默认的part-m-00000等文件，因为，数据已经交给MultipleOutputs输出了
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

        // 4、封装参数：本次job要处理的输入数据集所在路径、最终结果的输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);


    }
}
