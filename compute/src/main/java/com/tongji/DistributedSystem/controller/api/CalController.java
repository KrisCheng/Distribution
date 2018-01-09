package com.tongji.DistributedSystem.controller.api;

import com.tongji.CalculateSystem.job.TypeRateJob;
import com.tongji.DistributedSystem.response.BaseResp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

/**
 * Created by 秦博 on 2018/1/7.
 */

@RestController
@RequestMapping(value = "/api/cal")
public class CalController {

    @RequestMapping(value = "/test", method = RequestMethod.GET)
    public BaseResp test() throws IOException {
        int result = 0;
        String [] args = {"11", "22"};
        try {
            result = ToolRunner.run(new Configuration(), new TypeRateJob(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new BaseResp(BaseResp.SUCCESS);
    }

}
