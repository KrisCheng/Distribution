package com.tongji.DistributedSystem.util;

import com.google.common.io.Resources;
import com.tongji.DistributedSystem.mapper.UserCallNumMapper;
import com.tongji.DistributedSystem.reducer.UserCallNumReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 * Created by 秦博 on 2017/12/26.
 */
public class MapReduceOperator {

    static Configuration conf = new Configuration();

    static {
//        conf.set("fs.defaultFS", "hdfs://ha-master");
//        conf.set("dfs.nameservices", "ha-master");
//        conf.set("dfs.ha.namenodes.ha-master", "nn1,nn2,nn3,nn4");
//        conf.set("dfs.namenode.rpc-address.ha-master.nn1", "hd-data1:9000");
//        conf.set("dfs.namenode.rpc-address.ha-master.nn2", "hd-data2:9000");
//        conf.set("dfs.namenode.rpc-address.ha-master.nn3", "hd-data3:9000");
//        conf.set("dfs.namenode.rpc-address.ha-master.nn4", "hd-data4:9000");
//        conf.set("dfs.client.failover.proxy.provider.ha-master" ,"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        conf.addResource(Resources.getResource("core-site.xml"));
        conf.addResource(Resources.getResource("hdfs-site.xml"));
        conf.addResource(Resources.getResource("mapred-site.xml"));
    }
    public static void AvgCall(String src,String dst) throws Exception{
        Job job = Job.getInstance(conf, "AvgCall");
        job.setJarByClass(MapReduceOperator.class);
        job.setMapperClass(UserCallNumMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(UserCallNumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(src));
        FileOutputFormat.setOutputPath(job, new Path(dst));
        boolean success =  job.waitForCompletion(true);
        System.out.println(success);
    }


}
