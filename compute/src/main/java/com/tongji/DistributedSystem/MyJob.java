package com.tongji.DistributedSystem;

import com.google.common.io.Resources;

import com.tongji.DistributedSystem.mapper.UserCallNumMapper;
import com.tongji.DistributedSystem.reducer.UserCallNumReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class MyJob extends Configured implements Tool {
    Logger log = Logger.getLogger(MyJob.class);
    @Override
    public int run(String[] args) throws Exception {
        log.info("begin to run");
        Configuration conf = new Configuration();
        conf.addResource(Resources.getResource("core-site.xml"));
        conf.addResource(Resources.getResource("hdfs-site.xml"));
        conf.addResource(Resources.getResource("mapred-site.xml"));
        Job job = Job.getInstance(conf, "AvgCall");
        job.setJarByClass(MyJob.class);
        job.setMapperClass(UserCallNumMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(UserCallNumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("/user/data/omega.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/user/data/UserCallNum"));

        return job.waitForCompletion(true)?0:1;
    }
    public static void main(String [] args){
        int result = 0;
        try {
            result = ToolRunner.run(new Configuration(), new MyJob(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(result);
    }
}
