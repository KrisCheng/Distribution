package com.tongji.DistributedSystem;


import com.tongji.DistributedSystem.util.FileOperator;
import com.tongji.DistributedSystem.util.MapReduceOperator;

/**
 * Created by 秦博 on 2017/12/25.
 */
public class DistributedSystem {

    public static void main(String[] args) throws Exception {
//        //输入路径
//        String dst = "hdfs://localhost:9000/intput.txt";
//        //输出路径，必须是不存在的，空文件夹也不行。
//        String dstOut = "hdfs://localhost:9000/output";
//        Configuration hadoopConfig = new Configuration();
//
//        hadoopConfig.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
//
//        hadoopConfig.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
//        Job job = new Job(hadoopConfig);
//
//        //如果需要打成jar运行，需要下面这句
//        //job.setJarByClass(NewMaxTemperature.class);
//
//        //job执行作业时输入和输出文件的路径
//        FileInputFormat.addInputPath(job, new Path(dst));
//        FileOutputFormat.setOutputPath(job, new Path(dstOut));
//
//        //指定自定义的Mapper和Reducer作为两个阶段的任务处理类
//        job.setMapperClass(TestMapper.class);
//        job.setReducerClass(TestReducer.class);
//
//        //设置最后输出结果的Key和Value的类型
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);
//
//        //执行job，直到完成
//        job.waitForCompletion(true);
//        System.out.println("Finished");

//        try {
//            String localSrc = "D://lambda.txt";
//            String dst = "hdfs://hd-master:9000/home/hduser/hadoop/tmp/dfs/namesecondary/lambda.txt";
//            InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
//            Configuration conf = new Configuration();
//            FileSystem fs = FileSystem.get(URI.create(dst), conf);
//            OutputStream out = fs.create(new Path(dst), new Progressable() {
//                public void progress() {
//                    System.out.print(".");
//                }
//            });
//
//            IOUtils.copyBytes(in, out, 4096, true);
//
//            System.out.println("success");
//
//        } catch (Exception e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }
        /*String str = "20120201\t349723\ty24373194378\t1\t2\t0551\t0551\t0551\t0551\t08:39:50\t08:42:54\t184\t1\t373\t";
        String[] dictionary = str.split("\\s{2,}|\t");
        for(int i = 0;i<dictionary.length;i++){
            System.out.println(dictionary[i]);
        }*/
        /*try {
            System.out.println("文件创建成功！1");
            FileOperator.uploadFile("D://omega.txt", "/user/data/omega.txt");
            FileOperator.getDirectoryFromHdfs("/");
            System.out.println("文件创建成功！2");
        }catch (Exception e){
            e.printStackTrace();
        }*/
        try{
            MapReduceOperator.AvgCall("/user/data/omega.txt","/user/data/UserCallNum");
        }catch (Exception e){
            e.printStackTrace();
        }
    }

}
