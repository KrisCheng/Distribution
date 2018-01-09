package com.tongji.DistributedSystem.controller.api;

import com.tongji.DistributedSystem.request.fs.DeleteFileReq;
import com.tongji.DistributedSystem.request.fs.PostCreateFileReq;
import com.tongji.DistributedSystem.request.fs.PostMkdirReq;
import com.tongji.DistributedSystem.request.fs.PutRenameReq;
import com.tongji.DistributedSystem.response.BaseResp;
import com.tongji.DistributedSystem.service.FsService;
import com.tongji.DistributedSystem.tool.FileOperator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Created by 秦博 on 2017/12/31.
 */

@RestController
@RequestMapping(value = "/api/fs")
public class FsController {

    @Autowired
    FsService fsService;

    @RequestMapping(value = "/createFile", method = RequestMethod.POST)
    public BaseResp postCreateFile(@RequestBody PostCreateFileReq postCreateFileReq) throws IOException {
        return fsService.postCreateFile(postCreateFileReq.dst, postCreateFileReq.content);
    }

    @RequestMapping(value = "/downloadFile", method = RequestMethod.GET)
    public ResponseEntity<InputStreamResource> downloadFile(String filename, String dst) throws IOException {
        String filePath = fsService.getDownload(filename, dst);
        FileSystemResource file = new FileSystemResource(filePath);
        HttpHeaders headers = new HttpHeaders();
        headers.add("Cache-Control", "no-cache, no-store, must-revalidate");
        headers.add("Content-Disposition", String.format("attachment; filename=\"%s\"", file.getFilename()));
        headers.add("Pragma", "no-cache");
        headers.add("Expires", "0");

        return ResponseEntity
                .ok()
                .headers(headers)
                .contentLength(file.contentLength())
                .contentType(MediaType.parseMediaType("application/octet-stream"))
                .body(new InputStreamResource(file.getInputStream()));
    }

    @RequestMapping(value = "/uploadFile", method = RequestMethod.POST)
    public BaseResp postUpload(@RequestParam("file") MultipartFile file, String dst) throws IOException {
        try {
            //网站跟目录路径
            String webBaseDir = System.getProperty("user.dir");
            System.out.println(webBaseDir);
            //上传文件保存的目录路径
            String uploadFileDir = webBaseDir + "/tmp/";
            String fileName = file.getOriginalFilename();
            //生成上传文件的路径
            String filePath = uploadFileDir + "/" + fileName;
            //保存文件
            Files.copy(file.getInputStream(), Paths.get(filePath));
            fsService.postUpload(filePath, dst);
            FileOperator.deleteFile(filePath);
            return new BaseResp(BaseResp.SUCCESS);
        } catch (IOException |RuntimeException e) {
            return new BaseResp(BaseResp.FAILURE);
        }
    }

    @RequestMapping(value = "/rename", method = RequestMethod.PUT)
    public BaseResp putRename(@RequestBody PutRenameReq putRenameReq) throws IOException {
        return fsService.putRename(putRenameReq.oldName, putRenameReq.newName);
    }

    @RequestMapping(value = "/delete", method = RequestMethod.DELETE)
    public BaseResp deleteFile(@RequestBody DeleteFileReq deleteFileReq) throws IOException {
        return fsService.deleteFile(deleteFileReq.filePath);
    }

    @RequestMapping(value = "/mkdir", method = RequestMethod.POST)
    public BaseResp postMkdir(@RequestBody PostMkdirReq postMkdirReq) throws IOException {
        return fsService.postMkdir(postMkdirReq.path);
    }

    @RequestMapping(value = "/readFile", method = RequestMethod.GET)
    public BaseResp getReadFile(String filePath) throws IOException {
        return fsService.getReadFile(filePath);
    }

    @RequestMapping(value = "/getDirectoryFromHdfs", method = RequestMethod.GET)
    public BaseResp getDirectoryFromHdfs(String path) throws IOException {
        return fsService.getDirectoryFromHdfs(path);
    }

}
