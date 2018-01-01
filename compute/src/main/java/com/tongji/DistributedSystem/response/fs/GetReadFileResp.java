package com.tongji.DistributedSystem.response.fs;

import com.tongji.DistributedSystem.response.BaseResp;

/**
 * Created by 秦博 on 2018/1/1.
 */
public class GetReadFileResp extends BaseResp {

    public String content;

    public GetReadFileResp(int errCode, String content) {
        super(errCode);
        this.content = content;
    }
}
