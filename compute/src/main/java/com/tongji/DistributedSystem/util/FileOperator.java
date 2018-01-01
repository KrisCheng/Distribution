package com.tongji.DistributedSystem.util;

import com.google.common.io.Resources;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 * Created by 秦博 on 2017/12/26.
 */
public class FileOperator {

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

    //创建新文件
    public static void createFile(String dst , byte[] contents) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path dstPath = new Path(dst); //目标路径
        //打开一个输出流
        FSDataOutputStream outputStream = fs.create(dstPath);
        outputStream.write(contents);
        outputStream.close();
        fs.close();
        System.out.println("文件创建成功！");
    }

    //上传本地文件
    public static void uploadFile(String src,String dst) throws Exception{
        //Configuration conf = new Configuration();
        try {
            System.out.println("start");
            FileSystem fs = FileSystem.get(conf);
            System.out.println("start1");
            Path srcPath = new Path(src); //本地上传文件路径
            Path dstPath = new Path(dst); //hdfs目标路径
            //调用文件系统的文件复制函数,前面参数是指是否删除原文件，true为删除，默认为false
            System.out.println("start2");
            fs.copyFromLocalFile(false, srcPath, dstPath);
            System.out.println("start3");
            //打印文件路径
            System.out.println("Upload to " + conf.get("fs.default.name"));
            System.out.println("------------list files------------" + "\n");
            FileStatus[] fileStatus = fs.listStatus(dstPath);
            for (FileStatus file : fileStatus) {
                System.out.println(file.getLen());
                System.out.println(file.getPath());
            }
            fs.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    //文件重命名
    public static void rename(String oldName,String newName) throws IOException{
        //Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path oldPath = new Path(oldName);
        Path newPath = new Path(newName);
        boolean isok = fs.rename(oldPath, newPath);
        if(isok){
            System.out.println("rename ok!");
        }else{
            System.out.println("rename failure");
        }
        fs.close();
    }

    //删除文件
    public static void delete(String filePath) throws IOException{
        //Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(filePath);
        boolean isok = fs.deleteOnExit(path);
        if(isok){
            System.out.println("delete ok!");
        }else{
            System.out.println("delete failure");
        }
        fs.close();
    }

    //创建目录
    public static void mkdir(String path) throws IOException{
        //Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path srcPath = new Path(path);
        boolean isok = fs.mkdirs(srcPath);
        if(isok){
            System.out.println("create " + path + " dir ok!");
        }else{
            System.out.println("create " + path + " dir failure");
        }
        fs.close();
    }

    //读取文件的内容
    public static void readFile(String filePath) throws IOException{
        //Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path srcPath = new Path(filePath);
        InputStream in = null;
        try {
            in = fs.open(srcPath);
            IOUtils.copyBytes(in, System.out, 4096, false); //复制到标准输出流
        } finally {
            IOUtils.closeStream(in);
        }
    }

    /**
     * 遍历指定目录(direPath)下的所有文件
     */
    public static void  getDirectoryFromHdfs(String direPath){
        try {
            FileSystem fs = FileSystem.get(URI.create(direPath),conf);
            FileStatus[] filelist = fs.listStatus(new Path(direPath));
            for (int i = 0; i < filelist.length; i++) {
                System.out.println("_________" + direPath + "目录下所有文件______________");
                FileStatus fileStatus = filelist[i];
                System.out.println("Name:"+fileStatus.getPath().getName());
                System.out.println("Size:"+fileStatus.getLen());
                System.out.println("Path:"+fileStatus.getPath());
            }
            fs.close();
        } catch (Exception e){
            e.printStackTrace();
        }
    }

}
