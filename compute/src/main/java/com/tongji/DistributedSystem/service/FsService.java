package com.tongji.DistributedSystem.service;

import com.tongji.DistributedSystem.response.BaseResp;
import com.tongji.DistributedSystem.response.fs.GetDirectoryFromHdfsResp;
import com.tongji.DistributedSystem.response.fs.GetReadFileResp;
import com.tongji.DistributedSystem.tool.FileOperator;
import org.springframework.stereotype.Service;

import java.io.IOException;


/**
 * Created by 秦博 on 2017/12/31.
 */

@Service
public class FsService {

    public BaseResp postCreateFile(String dst, String content) throws IOException {
        return new BaseResp(FileOperator.createFile(dst, content));
    }

    public BaseResp postUpload(String src,String dst) throws IOException {
        return new BaseResp(FileOperator.uploadFile(src, dst));
    }

    public String getDownload(String filename, String dst) throws IOException {
        return FileOperator.downloadFile(filename, dst);
    }

    public BaseResp putRename(String oldName, String newName) throws IOException {
        return new BaseResp(FileOperator.rename(oldName, newName));
    }

    public BaseResp deleteFile(String filePath) throws IOException {
        return new BaseResp(FileOperator.delete(filePath));
    }

    public BaseResp postMkdir(String path) throws IOException {
        return new BaseResp(FileOperator.mkdir(path));
    }

    public BaseResp getReadFile(String filePath) throws IOException {
        return new GetReadFileResp(BaseResp.SUCCESS, FileOperator.readFile(filePath));
    }

    public BaseResp getDirectoryFromHdfs(String path) throws IOException {
        return new GetDirectoryFromHdfsResp(BaseResp.SUCCESS, FileOperator.getDirectoryFromHdfs(path));
    }

}
