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
        public long accessTime;
        public String owner;
        public String superGroup;
        public long blockSize;
        public long modificationTime;
        public int replication;
        public String permission;
        public boolean isFile;

        public Directory(String name, long length, String path, long accessTime, String owner, String superGroup, long blockSize, long modificationTime, int replication, String permission, boolean isFile) {
            this.name = name;
            this.length = length;
            this.path = path;
            this.accessTime = accessTime;
            this.owner = owner;
            this.superGroup = superGroup;
            this.blockSize = blockSize;
            this.modificationTime = modificationTime;
            this.replication = replication;
            this.permission = permission;
            this.isFile = isFile;
        }
    }

    public List<Directory> directories;

    public GetDirectoryFromHdfsResp(int errCode, List<Directory> directories) {
        super(errCode);
        this.directories = directories;
    }
}
