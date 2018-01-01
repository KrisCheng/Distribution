package com.tongji.DistributedSystem.response.fs;

import com.tongji.DistributedSystem.response.BaseResp;

import java.util.List;

/**
 * Created by 秦博 on 2017/12/31.
 */
public class GetDirectoryFromHdfsResp extends BaseResp {

    public static class Directory{
        public String name;
        public long length;
        public String path;

        public Directory(String name, long length, String path) {
            this.name = name;
            this.length = length;
            this.path = path;
        }
    }

    public List<Directory> directories;

    public GetDirectoryFromHdfsResp(int errCode, List<Directory> directories) {
        super(errCode);
        this.directories = directories;
    }
}
