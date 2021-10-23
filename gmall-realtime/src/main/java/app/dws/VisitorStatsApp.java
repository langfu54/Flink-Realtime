package app.dws;

import bean.VisitorStats;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ReduceApplyProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import utils.ClickHouseUtil;
import utils.DateTimeUtil;
import utils.MyKafkaUtil;

import java.time.Duration;
import java.util.Date;

/**
 * @Author:Langfu54@gmail.com
 * @Date:2021.08
 * @desc:
 */
public class VisitorStatsApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.创建执行环境，设置并行度
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 开启状态后端
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop101:8020/gmall-flink-210325/ck"));
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);
        //TODO 2.获取数据，分别构建数据来源，
            //  2.1 获取PV,SV,duration
        String sourceTopic = "dwd_page_log";
        String group = "VisitorStatsApp";
        DataStreamSource<String> pvStreamSource = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic,group));
            //  2.2 获取 UV
        String sourceTopic1 = "dwm_unique_visit";
        String group1 = "VisitorStatsApp";
        DataStreamSource<String> uvStreamSource = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic1,group1));
            // 2.3 获取 UJ
        String sourceTopic2 = "dwm_user_jump_detail";
        String group2 = "VisitorStatsApp";
        DataStreamSource<String> ujStreamSource = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic2,group2));

        //TODO 3。 Union数据，构建Bean - VisitorStats
            //3.1 构建PV,SV,duration
        SingleOutputStreamOperator<VisitorStats> visitorStatswithPV = pvStreamSource.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");
            long ts = Long.parseLong(jsonObject.getString("ts"));
            long sv = 0;
            if (jsonObject.getJSONObject("page").getString("last_page_id") == null || jsonObject.getJSONObject("page").getString("last_page_id").length() <= 0) {
                sv = 1L;
            }
            return new VisitorStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L,
                    1L,
                    sv,
                    0L,
                    jsonObject.getJSONObject("page").getLong("during_time"),
                    ts
            );
        });

        //3.2 构建UV
        SingleOutputStreamOperator<VisitorStats> visitorStatswithUV = uvStreamSource.map(line -> {
            JSONObject common = JSON.parseObject(line).getJSONObject("common");
            long ts = Long.parseLong(JSON.parseObject(line).getString("ts"));
            return new VisitorStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    1L,
                    0L,
                    0L,
                    0L,
                    0L,
                    ts
            );
        });
        System.out.println("UV");
        visitorStatswithUV.print();
        //3.3 构建UJ
        SingleOutputStreamOperator<VisitorStats> visitorStatswithUJ = uvStreamSource.map(line -> {
            JSONObject common = JSON.parseObject(line).getJSONObject("common");
            long ts = Long.parseLong(JSON.parseObject(line).getString("ts"));
            return new VisitorStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L,
                    0L,
                    0L,
                    1L,
                    0L,
                    ts
            );
        });


        //TODO 4.将数据进行Union
        DataStream<VisitorStats> visitorStatsDataStream = visitorStatswithPV.union(visitorStatswithUV, visitorStatswithUJ);

        //TODO 5.生成WM
        SingleOutputStreamOperator<VisitorStats> visitorStats = visitorStatsDataStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
                            @Override
                            public long extractTimestamp(VisitorStats element, long recordTimestamp) {
                                return element.getTs();
                            }
                        })
        );
        //TODO 6.分组聚合
        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> keyedStream = visitorStats.keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(VisitorStats value) throws Exception {
                return new Tuple4<String, String, String, String>(value.getAr(),
                        value.getCh(),
                        value.getIs_new(),
                        value.getVc());
            }
        });

        //TODO 7.开窗聚合
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> window = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)));

        SingleOutputStreamOperator<VisitorStats> visitorStatsDS = window.reduce(new ReduceFunction<VisitorStats>() {
            @Override
            public VisitorStats reduce(VisitorStats value1, VisitorStats value2) throws Exception {
                value1.setUv_ct(value1.getUv_ct() + value2.getUv_ct());
                value1.setPv_ct(value1.getPv_ct() + value2.getPv_ct());
                value1.setSv_ct(value1.getSv_ct() + value2.getSv_ct());
                value1.setUj_ct(value1.getUj_ct() + value2.getUj_ct());
                value1.setDur_sum(value1.getDur_sum() + value2.getDur_sum());

                return value1;
            }
        }, new WindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<VisitorStats> input, Collector<VisitorStats> out) throws Exception {
                long start = window.getStart();
                long end = window.getEnd();

                //获取迭代器值，只有一个，由增量聚合函数reduce聚合的结果，传到窗口，添加窗口信息。
                VisitorStats visitorStats = input.iterator().next();

                //补充窗口信息
                visitorStats.setStt(DateTimeUtil.toYMDhms(new Date(start)));
                visitorStats.setEdt(DateTimeUtil.toYMDhms(new Date(end)));

                out.collect(visitorStats);
            }
        });

        //TODO 8.写入到ClickHouse
        System.out.println(">>>>vs");
        visitorStatsDS.print();
//        visitorStatsDS.addSink(ClickHouseUtil.clickhouseSink("insert into visitor_stats_2021 values(?,?,?,?,?,?,?,?,?,?,?,?"));

        //TODO 9.执行
        env.execute("VisitorStatsApp");
    }
}
