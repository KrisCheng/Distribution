package com.tongji.CalculateSystem.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by 秦博 on 2017/12/25.
 */
public class UserDateMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String str = value.toString();
        str += "\r\n";
        String[] dictionary = str.split("\\s{2,}|\t");
        word.set(dictionary[0]+"\t"+dictionary[1]);
        context.write(word,one);
    }
}
