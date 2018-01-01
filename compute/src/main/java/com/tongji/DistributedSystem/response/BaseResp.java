package com.tongji.DistributedSystem.response;

/**
 * Created by 秦博 on 2017/12/31.
 */
public class BaseResp {
    // 成功
    public static final int SUCCESS = 0;

    // 失败
    public static final int FAILURE = 999;

    public int errCode;

    public BaseResp(int errCode){
        this.errCode = errCode;
    }

}
