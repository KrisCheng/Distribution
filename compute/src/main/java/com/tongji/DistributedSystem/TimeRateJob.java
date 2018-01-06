package com.tongji.DistributedSystem;

import com.tongji.DistributedSystem.bean.CallTypeJobBean;
import com.tongji.DistributedSystem.mapper.UserDateMapper;
import com.tongji.DistributedSystem.reducer.UserDateReducer;
import com.tongji.DistributedSystem.util.FileOperator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.*;

public class TimeRateJob extends Configured implements Tool {
    Logger log = Logger.getLogger(MyJob.class);
    String jar_path = "D:\\github\\compute\\build\\libs\\Distribution-1.0-SNAPSHOT.jar";
    String input_path = "/user/data/tb_call_201202_random.txt";
    String timeNum_path = "/user/data/TimeNum";
    String timeRate_path = "/user/data/TimeRate";
    String FilePath = "D:\\github\\var\\";

    public void getResult() throws Exception{
        FileOperator.downloadFile("TimeRate.txt", timeRate_path + "/part-r-00000");
    }
    public static int switchTimeSlot(String operator){
        if(operator.equals("00")||operator.equals("01")||operator.equals("02")){
            return 1;
        }
        else if(operator.equals("03")||operator.equals("04")||operator.equals("05")){
            return 2;
        }
        else if(operator.equals("06")||operator.equals("07")||operator.equals("08")){
            return 3;
        }
        else if(operator.equals("09")||operator.equals("10")||operator.equals("11")){
            return 4;
        }
        else if(operator.equals("12")||operator.equals("13")||operator.equals("14")){
            return 5;
        }
        else if(operator.equals("15")||operator.equals("16")||operator.equals("17")){
            return 6;
        }
        else if(operator.equals("18")||operator.equals("19")||operator.equals("20")){
            return 7;
        }
        else if(operator.equals("21")||operator.equals("22")||operator.equals("23")){
            return 8;
        }
        return 0;
    }
    public static class TimeNumMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        //String[] type = {"","localCall","longCall","roamCall"};
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String str = value.toString();System.out.println("start");
            str += "\r\n";
            String[] dictionary = str.split("\\s{2,}|\t");
            String timeStamp = dictionary[9].substring(0,2);
            // 1 --> 主叫号码 9 --> 开始时间 11 --> 通话时长
            context.write(new Text(dictionary[1]+"\t"+switchTimeSlot(timeStamp)),new IntWritable(Integer.parseInt(dictionary[11])));
        }
    }

    public static class TimeNumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
    public static class TimeRateMapper extends Mapper<Object, Text, Text, Text> {
        private final static IntWritable one = new IntWritable(1);
        //String[] type = {"","localCall","longCall","roamCall"};
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String str = value.toString();
            str += "\r\n";
            String[] dictionary = str.split("\\s{2,}|\t");
            System.out.println(str);
            System.out.println(dictionary.length);
            context.write(new Text(dictionary[0]),new Text(dictionary[1]+"\t"+dictionary[2]));
        }
    }

    public static class TimeRateReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] timeRate = new String[8];
            for(int i = 0;i < 8;i++){
                timeRate[i] = "0";
            }
            String str = null;
            for (Text val : values) {
                str = val.toString();
                str += "\r\n";
                String[] dictionary = str.split("\\s{2,}|\t");
                if(dictionary.length == 2){
                    timeRate[Integer.parseInt(dictionary[0])-1] = dictionary[1];
                }else{
                    for(int i = 0;i < 8;i++){
                        if(!dictionary[i].equals("0")){
                            timeRate[i] = dictionary[i];
                        }
                    }
                }
            }
            StringBuilder sb = new StringBuilder("");
            for(int i = 0;i < 8;i++){
                sb.append(timeRate[i]+"\t");
            }
            context.write(key,new Text(sb.toString()));
        }
//        protected void cleanup(Context context)throws IOException,InterruptedException{
//        }
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
        //FileOperator.delete(timeNum_path);
        FileOperator.delete(timeRate_path);

        Job job = Job.getInstance(conf, "TimeNum");
        job.setJarByClass(TimeRateJob.class);
        job.setJar(jar_path);
        job.setMapperClass(TimeNumMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(TimeNumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(input_path));
        FileOutputFormat.setOutputPath(job, new Path(timeNum_path));
        System.out.println("start");

        Job job2 = Job.getInstance(conf, "TimeRate");
        job2.setJarByClass(TimeRateJob.class);
        job2.setJar(jar_path);
        job2.setMapperClass(TimeRateMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job2.setReducerClass(TimeRateReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(timeNum_path));
        FileOutputFormat.setOutputPath(job2, new Path(timeRate_path));

        //if(job.waitForCompletion(true)){
            if(job2.waitForCompletion(true)){
                getResult();
            }
        //}

        System.out.println("start");
        return 0;
    }

    public static void main(String [] args){
        int result = 0;
        try {
            result = ToolRunner.run(new Configuration(), new TimeRateJob(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(result);
    }
}
