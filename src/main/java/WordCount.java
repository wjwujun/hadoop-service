import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;

public class WordCount {

    public static void main(String[] args) throws IOException, InterruptedException {
        String str="hdfs://192.168.72.130:9000/";

        /*
        *Configuration参数配置的机制：
        * 构造时候会默认加载默认配置中的xx-default.xml
        * 在加载时 会加载用户配置的xx-site.xml 覆盖掉默认配置
        * 构造完以后会，可以config.set("p","v"),会再次覆盖掉配置文件中的参数
        * */
        Configuration conf = new Configuration();
        //指定保存到hdfs上面的副本数量为2
        conf.set("dfs.replication","2");
        //指定客户端上传到hdfs的块大小64m
        conf.set("dfs.blocksize","64m");

        /*
        * 构造一个访问hdfs的客户端对象
        *  URI  hdfs客户端lockList
        *  conf 配置
        *  用户名
        * */
        FileSystem fs = FileSystem.get(URI.create(str), conf, "root");

        /*上传一个文件到hdfs中*/
        //fs.copyFromLocalFile(new Path("E:/qqfile/1477563131/FileRecv/MobileFile/1.jpg"),new Path("/"));

        /*下载一个文件*/
        //fs.copyToLocalFile(new Path("/IMG_20181212_230555.jpg"),new Path("E:/javaproject/hadoop-service"));

        /*hdfs内部 移动文件*/
        //fs.rename(new Path("/1.jpg"),new Path("/app/2.jpg"));

        /*创建文件夹(可创建多级)*/
        //fs.mkdirs(new Path("/test/aaa"));

        /*删除一个hdfs中的文件(递归删除)*/
        //fs.delete(new Path("/MobileFile/"),true);


        /*查询指定目录下的信息*/
      /*  RemoteIterator<LocatedFileStatus> iteraor = fs.listFiles(new Path("/"), true);
        while (iteraor.hasNext()){
            LocatedFileStatus next = iteraor.next();

            System.out.println("块大小"+next.getBlockSize());
            System.out.println("块长度"+next.getLen());
            System.out.println("副本数量"+next.getReplication());
            System.out.println("块信息"+ Arrays.toString(next.getBlockLocations()));
            System.out.println("------------------------------------------------------");
        }*/



        /*查询目下的信息*/
   /*     FileStatus[] status = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : status) {
            System.out.println("路径"+fileStatus.getPath());
            System.out.println("这个文件是文件夹吗？"+fileStatus.isDirectory());
            System.out.println("这个文件是文件吗？"+fileStatus.isFile());
            System.out.println("块大小"+fileStatus.getBlockSize());
            System.out.println("块长度"+fileStatus.getLen());
            System.out.println("------------------------------------------------------");
        }*/



        /*读取hdfs中的文件的内容*/
       /* FSDataInputStream in = fs.open(new Path("/a.txt"));
        BufferedReader br = new BufferedReader(new InputStreamReader(in,"utf8"));  //包装成带缓冲的字符流,指定编码
        String line=null;
        while((line=br.readLine())!=null){
            System.out.println(line);

        }
        br.close();
        in.close();*/


       /*读取指定字节数*/
     /* FSDataInputStream in = fs.open(new Path("/a.txt"));
        in.seek(6);  //指定读取起始位置
        byte[] buf = new byte[16];  //读取字节数
        in.read(buf);
        System.out.println(new String(buf));
        in.close();*/



        /*往hdfs中的文件的写内容*/
        FSDataOutputStream out = fs.create(new Path("/c.txt"), false);//追加
        byte[] buf = new byte[1024];

        FileInputStream in = new FileInputStream("E:/javaproject/a.txt");
        int read =0;
        while ((read=in.read(buf))!=-1){
            out.write(buf,0,read);      //最后一次可能没有1024
        }
        in.close();
        out.close();


        fs.close();

    }


}
