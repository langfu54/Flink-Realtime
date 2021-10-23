package com.fulang.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.fulang.publisher.service.ProductStatsService;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;

/**
 * @Author:Langfu54@gmail.com
 * @Date:2021.09
 * @desc:
 */
public class SugarController {
    @Autowired
    ProductStatsService productStatsService;

    /*
    {
        "status": 0,
        "msg": "",
        "data": 1201081.1632389291
    }
     */
    @RequestMapping("/gmv")
    public String getGMV(@RequestParam(value = "date",defaultValue = "0") Integer date) {
        if(date==0){
            date=now();
        }
        BigDecimal gmv = productStatsService.getGMV(date);
        HashMap res = new HashMap<String,Object>();
        res.put("statua",0);
        res.put("data",gmv);
        return JSON.toJSONString(res);
//        String json = "{   \"status\": 0,  \"data\":" + gmv + "}";
    }

    private int now(){
        String yyyyMMdd = DateFormatUtils.format(new Date(), "yyyyMMdd");
        return   Integer.valueOf(yyyyMMdd);
    }

}
