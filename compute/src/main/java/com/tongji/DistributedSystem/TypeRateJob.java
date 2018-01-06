package com.tongji.DistributedSystem;

import com.tongji.DistributedSystem.bean.CallTypeBean;
import com.tongji.DistributedSystem.bean.CallTypeJobBean;
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
import org.apache.hadoop.io.IntWritable;
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

public class TypeRateJob extends Configured implements Tool {
    Logger log = Logger.getLogger(MyJob.class);
    String jar_path = "D:\\github\\compute\\build\\libs\\Distribution-1.0-SNAPSHOT.jar";
    String input_path = "/user/data/tb_call_201202_random.txt";
    String typeRate_path = "/user/data/TypeRate";
    String FilePath = "D:\\github\\var\\";
    public static class TypeRateMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        //String[] type = {"","localCall","longCall","roamCall"};
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String str = value.toString();System.out.println("start");
            str += "\r\n";
            String[] dictionary = str.split("\\s{2,}|\t");
             // 3 --> 主叫号码运营商 4 --> 被叫号码运营商 12 --> 通话类型
                //以被叫号码运营商为准
            context.write(new Text(dictionary[12]),one);
            context.write(new Text(dictionary[12]+"\t"+dictionary[4]),one);
            //context.write(new Text(type[Integer.parseInt(dictionary[12])]), one);
            //context.write(new Text(type[Integer.parseInt(dictionary[12])]+"\t"+dictionary[4]), one);
        }
    }

    public static class TypeRateReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
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
        FileOperator.delete(typeRate_path);

        Job job = Job.getInstance(conf, "TypeRateNum");
        job.setJarByClass(TypeRateJob.class);
        job.setJar(jar_path);
        job.setMapperClass(TypeRateMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(TypeRateReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(input_path));
        FileOutputFormat.setOutputPath(job, new Path(typeRate_path));
        System.out.println("start");

        if(job.waitForCompletion(true)){
            computeTypeRate();
        }

        System.out.println("start");
        return 0;
    }

    private void computeTypeRate() {
        String[] type = {"","localCall","longCall","roamCall"};
        CallTypeJobBean[] callTypeJobBeans  = new CallTypeJobBean[4];
        for (int i = 0;i <= 3;i++){
            callTypeJobBeans[i] = new CallTypeJobBean();
        }
        try {
            FileOperator.downloadFile("TypeRate.txt", typeRate_path + "/part-r-00000");
            FileReader reader1 = new FileReader(FilePath + "TypeRate.txt");
            BufferedReader br1 = new BufferedReader(reader1);
            String str1 = null;
            File writename = new File(FilePath+"TypeRateResult.txt");
            writename.createNewFile();
            BufferedWriter out = new BufferedWriter(new FileWriter(writename));
            while((str1 = br1.readLine()) != null) {
                String[] dictionary1 = str1.split("\\s{2,}|\t");
                System.out.println(""+dictionary1.length);
                if(dictionary1.length == 2){
                    callTypeJobBeans[Integer.parseInt(dictionary1[0])].all = Double.parseDouble(dictionary1[1]);
                }else if(dictionary1.length == 3){
                    callTypeJobBeans[Integer.parseInt(dictionary1[0])].operate[Integer.parseInt(dictionary1[1])] = Double.parseDouble(dictionary1[2]);
                }
            }
            out.write("市话分布 -- " + "总计: " + callTypeJobBeans[1].all +" 移动: " + callTypeJobBeans[1].operate[1] + " 联通: " + callTypeJobBeans[1].operate[2] + " 电信: " + callTypeJobBeans[1].operate[3] + " 其他: " + callTypeJobBeans[1].operate[4]+"\r\n") ;
            out.write("长途分布 -- " + "总计: " + callTypeJobBeans[2].all+ " 移动: " + callTypeJobBeans[2].operate[1] + " 联通: " + callTypeJobBeans[2].operate[2] + " 电信: " + callTypeJobBeans[2].operate[3] + " 其他: " + callTypeJobBeans[2].operate[4]+"\r\n" );
            out.write("国际分布 -- " + "总计: " + callTypeJobBeans[3].all+ " 移动: " + callTypeJobBeans[3].operate[1] + " 联通: " + callTypeJobBeans[3].operate[2] + " 电信: " + callTypeJobBeans[3].operate[3] + " 其他: " + callTypeJobBeans[3].operate[4]+"\r\n");
            out.flush();
            out.close();
            reader1.close();
            br1.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void main(String [] args){
        int result = 0;
        try {
            //result = ToolRunner.run(new Configuration(), new TypeRateJob(), args);
            TypeRateJob typeRateJob = new TypeRateJob();
            typeRateJob.computeTypeRate();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(result);
    }
}
