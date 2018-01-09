package com.tongji.DistributedSystem.tool;

import com.google.common.io.Resources;
import com.tongji.DistributedSystem.response.BaseResp;
import com.tongji.DistributedSystem.response.fs.GetDirectoryFromHdfsResp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by 秦博 on 2017/12/26.
 */
public class FileOperator {

    static Configuration conf = new Configuration();

    static {
        conf.addResource(Resources.getResource("core-site.xml"));
        conf.addResource(Resources.getResource("hdfs-site.xml"));
        conf.addResource(Resources.getResource("mapred-site.xml"));
    }

    //创建新文件
    public static int createFile(String dst , String contents) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path dstPath = new Path(dst); //目标路径
        //打开一个输出流
        FSDataOutputStream outputStream = fs.create(dstPath);
        outputStream.write(contents.getBytes());
        outputStream.close();
        fs.close();
        System.out.println("文件创建成功！");
        return BaseResp.SUCCESS;
    }

    //上传本地文件
    public static int uploadFile(String src, String dst) throws IOException{
        //Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path srcPath = new Path(src); //本地上传文件路径
        Path dstPath = new Path(dst); //hdfs目标路径
        //调用文件系统的文件复制函数,前面参数是指是否删除原文件，true为删除，默认为false
        fs.copyFromLocalFile(false, srcPath, dstPath);

        //打印文件路径
        System.out.println("Upload to "+conf.get("fs.default.name"));
        System.out.println("------------list files------------"+"\n");
        FileStatus[] fileStatus = fs.listStatus(dstPath);
        for (FileStatus file : fileStatus)
        {
            System.out.println(file.getLen());
            System.out.println(file.getPath());
        }
        fs.close();
        return BaseResp.SUCCESS;
    }

    //下载本地文件
    public static String downloadFile(String filename, String dst) throws IOException{
        FileSystem fs = FileSystem.get(conf);
        InputStream in = fs.open(new Path(dst + filename));
        //网站跟目录路径
        String webBaseDir = System.getProperty("user.dir");
        //上传文件保存的目录路径
        String downloadFileDir = webBaseDir + "/tmp/";
        OutputStream out = new FileOutputStream(downloadFileDir + filename);
        IOUtils.copyBytes(in, out, 4096, true);
        fs.close();
        return downloadFileDir + filename;
    }

    //文件重命名
    public static int rename(String oldName,String newName) throws IOException{
        //Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path oldPath = new Path(oldName);
        Path newPath = new Path(newName);
        boolean isok = fs.rename(oldPath, newPath);
        if(isok){
            System.out.println("rename ok!");
            fs.close();
            return BaseResp.SUCCESS;
        }else {
            System.out.println("rename failure");
            fs.close();
            return BaseResp.FAILURE;
        }
    }

    //删除文件
    public static int delete(String filePath) throws IOException{
        //Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(filePath);
        boolean isok = fs.deleteOnExit(path);
        if(isok){
            System.out.println("delete ok!");
            fs.close();
            return BaseResp.SUCCESS;
        }else{
            System.out.println("delete failure");
            fs.close();
            return BaseResp.FAILURE;
        }
    }

    //创建目录
    public static int mkdir(String path) throws IOException{
        //Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path srcPath = new Path(path);
        boolean isok = fs.mkdirs(srcPath);
        if(isok){
            System.out.println("create " + path + " dir ok!");
            fs.close();
            return BaseResp.SUCCESS;
        }else{
            System.out.println("create " + path + " dir failure");
            fs.close();
            return BaseResp.FAILURE;
        }
    }

    //读取文件的内容
    public static String readFile(String filePath) throws IOException{
        //Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path srcPath = new Path(filePath);
        InputStream in = null;
        try {
            in = fs.open(srcPath);
            IOUtils.copyBytes(in, System.out, 4096, false); //复制到标准输出流
            return org.apache.commons.io.IOUtils.toString(in, "utf-8");
        } finally {
            IOUtils.closeStream(in);
        }
    }

    /**
     * 遍历指定目录(direPath)下的所有文件
     */
    public static List<GetDirectoryFromHdfsResp.Directory> getDirectoryFromHdfs(String direPath){
        try {
            List<GetDirectoryFromHdfsResp.Directory> directoryList = new ArrayList<>();
            FileSystem fs = FileSystem.get(URI.create(direPath),conf);
            FileStatus[] filelist = fs.listStatus(new Path(direPath));
            for (int i = 0; i < filelist.length; i++) {
                System.out.println("_________" + direPath + "目录下所有文件______________");
                FileStatus fileStatus = filelist[i];
                directoryList.add(new GetDirectoryFromHdfsResp.Directory(fileStatus.getPath().getName(), fileStatus.getLen(), fileStatus.getPath().toString(), fileStatus.getAccessTime(),
                        fileStatus.getOwner(), fileStatus.getGroup(), fileStatus.getBlockSize(), fileStatus.getModificationTime(), fileStatus.getReplication(), fileStatus.getPermission().toString(), fileStatus.isFile()));
            }
            fs.close();
            return directoryList;
        } catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

    public static boolean deleteFile(String fileName) {
        File file = new File(fileName);

        if (file.exists() && file.isFile()) {
            if (file.delete()) {
                System.out.println("delete" + fileName + " success!");
                return true;
            } else {
                System.out.println("delete" + fileName + " fail!");
                return false;
            }
        } else {
            System.out.println("delete:" + fileName + " not exist!");
            return false;
        }
    }

}
