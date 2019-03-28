package top;


import java.io.IOException;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class pageTopReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    //声明map,存储topn的数据,自动排序
    TreeMap<PageCount,Object> treeMap=new TreeMap<>();


    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count=0;
        for (IntWritable value : values) {
            count+=value.get();
        }
        PageCount pageCount = new PageCount(key.toString(),count);

        treeMap.put(pageCount,null);
        //reduce执行完了以后，不写入文件,交给cleanup做排序处理
        //context.write(key,new IntWritable(count));
    }


    /*
    * cleanup在reduce执行完以后，执行
    * */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        //从配置文件中取出，要获取的参数，一般要指定一个默认值。
        Configuration conf = context.getConfiguration();
        int top = (int) conf.getInt("top.n",5);

        Set<Entry<PageCount, Object>> entries = treeMap.entrySet();

        int i=0;
        for (Entry<PageCount, Object> entry : entries) {
            context.write(new Text(entry.getKey().getPage()),new IntWritable(entry.getKey().getCount()));
            i++;
            if(top==i) return;
        }

    }
}
