package com.fulang;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @Author:Langfu54@gmail.com
 * @Date:
 * @desc:
 */
public class FlinkCDCwithCustomDeserilizer {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境-设置并行度
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.定义flinkCDC Source
        SourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop101")
                .port(3306)
                .databaseList("rt-gmall2021") // monitor all tables under inventory database
                .tableList("rt-gmall2021.base_trademark")
                .username("root")
                .password("1234")
                .startupOptions(StartupOptions.initial())
                .deserializer(new CustomDeserilizer()) // converts SourceRecord to String
                .build();
        //3.添加Source
        DataStreamSource<String> streamSource = env.addSource(sourceFunction);

        streamSource.print();
        //4.执行Env,并为Job指定名称

        env.execute("gmall-cdc");
    }
}
