package com.fulang;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @Author:Langfu54@gmail.com
 * @Date:2021.08
 * @desc:
 */
public class FlinkCDC {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境-设置并行度
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 开启状态后端
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop101:8020/gmall-flink-210325/ck"));
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);


        //2.定义flinkCDC Source
        SourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop101")
                .port(3306)
                .databaseList("rt-gmall2021") // monitor all tables under inventory database
                .tableList("rt-gmall2021.base_trademark")  //需要使用 库.表名
                .username("root")
                .password("1234")
                .startupOptions(StartupOptions.initial())
                .deserializer(new StringDebeziumDeserializationSchema()) // converts SourceRecord to String
                .build();
        //3.添加Source
        DataStreamSource<String> streamSource = env.addSource(sourceFunction);

        streamSource.print();
        //4.执行Env,并为Job指定名称

        env.execute("gmall-cdc");
    }
}
