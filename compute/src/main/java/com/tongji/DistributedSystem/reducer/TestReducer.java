package com.tongji.DistributedSystem.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by 秦博 on 2017/12/25.
 */
public class TestReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int maxValue = Integer.MIN_VALUE;
        StringBuffer sb = new StringBuffer();
        //取values的最大值
        for (IntWritable value : values) {
            maxValue = Math.max(maxValue, value.get());
            sb.append(value).append(", ");
        }
        // 打印样本： Before Reduce: 2000, 15, 23, 99, 12, 22,
        System.out.print("Before Reduce: " + key + ", " + sb.toString());
        context.write(key, new IntWritable(maxValue));
        // 打印样本： After Reduce: 2000, 99
        System.out.println("======" + "After Reduce: " + key + ", " + maxValue);
    }
}
