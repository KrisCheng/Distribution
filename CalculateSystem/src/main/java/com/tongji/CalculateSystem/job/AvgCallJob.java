package com.tongji.CalculateSystem.job;

import com.google.common.io.Resources;
import com.tongji.CalculateSystem.mapper.UserCallNumMapper;
import com.tongji.CalculateSystem.mapper.UserDateMapper;
import com.tongji.CalculateSystem.mapper.UserDateNumMapper;
import com.tongji.CalculateSystem.reducer.UserCallNumReducer;
import com.tongji.CalculateSystem.reducer.UserDateNumReducer;
import com.tongji.CalculateSystem.reducer.UserDateReducer;
import com.tongji.CalculateSystem.util.FileOperator;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.*;

/**
 * Created by 秦博 on 2018/1/7.
 */
public class AvgCallJob extends Configured implements Tool {

    String input_path = "/user/data/tb_call_201202_random.txt";
    String UserCallNum_path = "/user/data/UserCallNum";
    String UserDate_path = "/user/data/UserDate";
    String UserDateNum_path = "/user/data/UserDateNum";
    String AvgCall_path = "/user/data/AvgCall";
    String FilePath = System.getProperty("user.dir") + "/tmp/";

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
        Configuration conf = new Configuration();
        conf.addResource(Resources.getResource("core-site.xml"));
        conf.addResource(Resources.getResource("hdfs-site.xml"));
        conf.addResource(Resources.getResource("mapred-site.xml"));
        conf.addResource(Resources.getResource("yarn-site.xml"));
        FileOperator.delete(UserDate_path);
        FileOperator.delete(UserCallNum_path);
        FileOperator.delete(UserDateNum_path);
        FileOperator.delete(AvgCall_path);

        Job job = Job.getInstance(conf, "UserCallNum");
        job.setJarByClass(AvgCallJob.class);
        job.setMapperClass(UserCallNumMapper.class);
        job.setReducerClass(UserCallNumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(input_path));
        FileOutputFormat.setOutputPath(job, new Path(UserCallNum_path));
        System.out.println("start");

        Job job2 = Job.getInstance(conf, "UserDate");
        job2.setJarByClass(AvgCallJob.class);
        job2.setMapperClass(UserDateMapper.class);
        job2.setReducerClass(UserDateReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path(input_path));
        FileOutputFormat.setOutputPath(job2, new Path(UserDate_path));

        Job job3 = Job.getInstance(conf, "UserDateNum");
        job3.setJarByClass(AvgCallJob.class);
        job3.setMapperClass(UserDateNumMapper.class);
        job3.setReducerClass(UserDateNumReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job3, new Path(UserDate_path));
        FileOutputFormat.setOutputPath(job3, new Path(UserDateNum_path));

        Job job4 = Job.getInstance(conf, "AvgCall");
        job4.setJarByClass(AvgCallJob.class);
        job4.setMapperClass(AvgCallMapper.class);
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
//                    FileOperator.downloadFile("AvgCall.txt",AvgCall_path + "/part-r-00000");
                }
            }
        }

        System.out.println("start");
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int result = 0;
        try {
            result = ToolRunner.run(new Configuration(), new AvgCallJob(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(result);
    }

}
