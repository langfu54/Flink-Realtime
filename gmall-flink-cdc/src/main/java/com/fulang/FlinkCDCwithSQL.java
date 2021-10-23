package com.fulang;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
/**
 * @Author:Langfu54@gmail.com
 * @Date:2021.08
 * @desc:
 */
public class FlinkCDCwithSQL {
    public static void main(String[] args) {
        //1.构建StreamEnv
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //1.1获取TABLE执行环境


        //2.

    }
}
