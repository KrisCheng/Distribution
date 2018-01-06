package com.tongji.DistributedSystem.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by 秦博 on 2017/12/25.
 */
public class UserCallNumMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String str = value.toString();System.out.println("start");
        str += "\r\n";
        String[] dictionary = str.split("\\s{2,}|\t");
        word.set(dictionary[1]);
        context.write(word,one);
    }
}
