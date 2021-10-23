package app.dws;

import bean.ProvinceStats;
import common.GmallConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.hadoop.hbase.client.ClientUtil;
import utils.ClickHouseUtil;
import utils.MyKafkaUtil;

/**
 * @Author:Langfu54@gmail.com
 * @Date:2021.09
 * @desc:
 */
public class ProvinceStatsSqlApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.创建执行环境，设置并行度
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //TODO 创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 开启状态后端
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop101:8020/gmall-flink-210325/ck"));
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);
        //TODO 2.获取数据，分别构建数据来源，
        String topic = "dws_order_wide";
        String group  = "provinceApp";
        TableResult tableResult = tableEnv.executeSql(
                "CREATE TABLE KafkaTable ( " +
                        "  `province_id` BIGINT, " +
                        "  `province_name` STRING, " +
                        "  `province_area_code` STRING, " +
                        "  `province_iso_code` STRING, " +
                        "  `province_3166_2_code` STRING, " +
                        "  `order_id` BIGINT, " +
                        "  `split_total_amount` DECIMAL, " +
                        "  `create_time` STRING, " +
                        "  `rt` as TO_TIMESTAMP(create_time), " +
                        "  WATERMARK FOR rt AS rt - INTERVAL '1' SECOND ) WITH ( "  +
                        MyKafkaUtil.KafkaConstant(topic,group));

        //TODO 3.执行查询语句

        Table sqlQuery = tableEnv.sqlQuery(" select  " +
                "        DATE_FORMAT(TUMBLE_START(rt, INTERVAL '10' second), 'yyyy-MM-dd hh:mm:ss')  stt, " +
                "        DATE_FORMAT(TUMBLE_END(rt, INTERVAL '10' second), 'yyyy-MM-dd hh:mm:ss') edt, " +
                "        province_id, " +
                "        province_name, " +
                "        province_area_code, " +
                "        province_iso_code, " +
                "        province_3166_2_code, " +
                "        count(order_id) order_count, " +
                "        sum(split_total_amount) order_amount, " +
                "       UNIX_TIMESTAMP()*1000 ts " +
                "    from KafkaTable " +
                "    group by " +
                "        TUMBLE(rt, INTERVAL '10' second), " +
                "         province_id, " +
                "        province_name, " +
                "        province_area_code, " +
                "        province_iso_code, " +
                "        province_3166_2_code");
        //TODO 4 .写入CH
        DataStream<ProvinceStats> provinceDS = tableEnv.toAppendStream(sqlQuery, ProvinceStats.class);

        provinceDS.print();

        ClickHouseUtil.clickhouseSink("insert into province_stats_210325 values(?,?,?,?,?,?,?,?,?,?)");
        //TODO 5. 执行
        
        env.execute("ProvinceStatsSqlApp");

    }
}
