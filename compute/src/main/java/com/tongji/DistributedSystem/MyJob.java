package com.tongji.DistributedSystem;

import com.google.common.io.Resources;

import com.tongji.DistributedSystem.mapper.UserCallNumMapper;
import com.tongji.DistributedSystem.mapper.UserDateMapper;
import com.tongji.DistributedSystem.mapper.UserDateNumMapper;
import com.tongji.DistributedSystem.reducer.UserCallNumReducer;
import com.tongji.DistributedSystem.reducer.UserDateNumReducer;
import com.tongji.DistributedSystem.reducer.UserDateReducer;
import com.tongji.DistributedSystem.util.FileOperator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.*;

public class MyJob extends Configured implements Tool {
    Logger log = Logger.getLogger(MyJob.class);
    String jar_path = "D:\\github\\compute\\build\\libs\\Distribution-1.0-SNAPSHOT.jar";
    String input_path = "/user/data/tb_call_201202_random.txt";
    String UserCallNum_path = "/user/data/UserCallNum";
    String UserDate_path = "/user/data/UserDate";
    String UserDateNum_path = "/user/data/UserDateNum";
    String AvgCall_path = "/user/data/AvgCall";
    String FilePath = "D:\\github\\var\\";

    public static class AvgCallMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        //String[] type = {"","localCall","longCall","roamCall"};
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String str = value.toString();System.out.println("start");
            str += "\r\n";
            String[] dictionary = str.split("\\s{2,}|\t");
            context.write(new Text(dictionary[0]),new DoubleWritable(Double.parseDouble(dictionary[1])));
        }
    }

    public static class AvgCallReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double []callDateNum = new double[2];
            int i = 0;
            for (DoubleWritable val : values) {
                callDateNum[i++] = val.get();
            }
            double max = callDateNum[0] > callDateNum[1]?callDateNum[0]:callDateNum[1];
            double min = callDateNum[0] < callDateNum[1]?callDateNum[0]:callDateNum[1];
            result.set(max/min);
            context.write(key, result);
        }
    }
    @Override
    public int run(String[] args) throws Exception {
        log.info("begin to run");
        Configuration conf = new Configuration();
        // this should be like defined in your yarn-site.xml
        //conf.set("mapred.jobtracker.address", "hd-master:54311");
        conf.set("yarn.resourcemanager.address", "hd-master:54311");
        conf.set("mapreduce.app-submission.cross-platform", "true");
        // framework is now "yarn", should be defined like this in mapred-site.xm
        conf.set("mapreduce.framework.name", "yarn");
        // like defined in hdfs-site.xml
        conf.set("fs.default.name", "hdfs://hd-master:54310");
        //conf.addResource(Resources.getResource("core-site.xml"));
        //conf.addResource(Resources.getResource("hdfs-site.xml"));
        //conf.addResource(Resources.getResource("mapred-site.xml"));
        FileOperator.delete(UserDate_path);
        FileOperator.delete(UserCallNum_path);
        FileOperator.delete(UserDateNum_path);
        FileOperator.delete(AvgCall_path);

        Job job = Job.getInstance(conf, "UserCallNum");
        job.setJarByClass(MyJob.class);
        job.setJar(jar_path);
        job.setMapperClass(UserCallNumMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(UserCallNumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(input_path));
        FileOutputFormat.setOutputPath(job, new Path(UserCallNum_path));
        System.out.println("start");

        Job job2 = Job.getInstance(conf, "UserDate");
        job2.setJarByClass(MyJob.class);
        job2.setJar(jar_path);
        job2.setMapperClass(UserDateMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job2.setReducerClass(UserDateReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path(input_path));
        FileOutputFormat.setOutputPath(job2, new Path(UserDate_path));

        Job job3 = Job.getInstance(conf, "UserDateNum");
        job3.setJarByClass(MyJob.class);
        job3.setJar(jar_path);
        job3.setMapperClass(UserDateNumMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job3.setReducerClass(UserDateNumReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job3, new Path(UserDate_path));
        FileOutputFormat.setOutputPath(job3, new Path(UserDateNum_path));

        Job job4 = Job.getInstance(conf, "AvgCall");
        job4.setJarByClass(MyJob.class);
        job4.setJar(jar_path);
        job4.setMapperClass(AvgCallMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job4.setReducerClass(AvgCallReducer.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job4, new Path(UserCallNum_path));
        FileInputFormat.addInputPath(job4, new Path(UserDateNum_path));
        FileOutputFormat.setOutputPath(job4, new Path(AvgCall_path));
        if(job2.waitForCompletion(true)){
            if(job.waitForCompletion(true) && job3.waitForCompletion(true)){
                //computeAvgCall();
                if (job4.waitForCompletion(true)){
                    FileOperator.downloadFile("AvgCall.txt",AvgCall_path + "/part-r-00000");
                }
            }
        }

        System.out.println("start");
        return 0;
    }

    private void computeAvgCall() {
        try {
            FileOperator.downloadFile("UserCallNum.txt",UserCallNum_path + "/part-r-00000");
            FileOperator.downloadFile("UserDateNum.txt",UserDateNum_path + "/part-r-00000");
            FileReader reader1 = new FileReader(FilePath + "UserCallNum.txt");
            FileReader reader2 = new FileReader(FilePath + "UserDateNum.txt");
            BufferedReader br1 = new BufferedReader(reader1);
            BufferedReader br2 = new BufferedReader(reader2);
            String str1 = null;
            String str2 = null;

            File writename = new File(FilePath+"avg_call.txt");
            writename.createNewFile();
            BufferedWriter out = new BufferedWriter(new FileWriter(writename));
            out.write("TeleNumber" + "\t" + "Count" + "\r\n");
            while((str1 = br1.readLine()) != null && (str2 = br2.readLine()) != null) {
                String[] dictionary1 = str1.split("\\s{2,}|\t");
                String[] dictionary2 = str2.split("\\s{2,}|\t");
                out.write(dictionary1[0]+"\t"+Float.parseFloat(dictionary1[1])/Float.parseFloat(dictionary2[1])+"\r\n");
            }
            out.flush();
            out.close();
            reader1.close();
            reader2.close();
            br1.close();
            br2.close();

        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void main(String [] args){
        int result = 0;
        try {
            result = ToolRunner.run(new Configuration(), new MyJob(), args);
//            MyJob my = new MyJob();
//            my.computeAvgCall();
//            System.out.println(System.getProperty("user.dir"));
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(result);
    }
}
