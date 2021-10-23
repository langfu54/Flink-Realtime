package com.fulang.gmall.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import lombok.extern.slf4j.Slf4j;
/**
 * @Author:Langfu54@gmail.com
 * @Date:2021.08
 * @desc:
 */
@Slf4j
@RestController  // @RestController = @Controller + @ResponseBody
public class LoggerController {
    @Autowired
    private KafkaTemplate<String,String> kafkaTemplate;
    @RequestMapping("applog")
    public String getLogger(@RequestParam("Param") String jsonStr){
        //打印log
        log.info(jsonStr);

        //写JsonStr到ODS主题
        kafkaTemplate.send("ods_base_log",jsonStr);
        return "success!";
    }

    //test with param
    @RequestMapping("test")
    public String test01(@RequestParam(value = "age",defaultValue = "18") int age,
                         @RequestParam("age") String name){
        return "success!";
    }
}
