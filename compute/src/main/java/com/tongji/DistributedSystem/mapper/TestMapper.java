package com.tongji.DistributedSystem.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by 秦博 on 2017/12/25.
 */
public class TestMapper extends Mapper<Object, Text, Text, IntWritable> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        System.out.print("Before Mapper: " + key + ", " + value);
        String line = value.toString();
        String year = line.substring(0, 4);
        int temperature = Integer.parseInt(line.substring(8));
        context.write(new Text(year), new IntWritable(temperature));
        // 打印样本: After Mapper:2000, 15
        System.out.println("======" + "After Mapper:" + new Text(year) + ", " + new IntWritable(temperature));
    }

}
