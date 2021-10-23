package app.dwm;

import bean.OrderWide;
import bean.PaymentInfo;
import bean.PaymentWide;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import utils.MyKafkaUtil;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;

/**
 * @Author:Langfu54@gmail.com
 * @Date:2021.08
 * @desc:
 */
public class PaymentWideApp {
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
        //TODO 2.获取dwm_order_wide主题数据,并转换为bean
        String sourceTopic = "dwm_order_wide";
        String group = "PaymentWideApp";
        DataStreamSource<String> streamSource = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic,group));

        SingleOutputStreamOperator<OrderWide> orderWideDS = streamSource.map(line -> {
            OrderWide orderWide = JSON.parseObject(line, OrderWide.class);
            return orderWide;

        }).assignTimestampsAndWatermarks(
                WatermarkStrategy.<OrderWide>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                            @Override
                            public long extractTimestamp(OrderWide element, long recordTimestamp) {
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
                                try {
                                    return sdf.parse(element.getCreate_date()).getTime();
                                } catch (ParseException e) {
                                    e.printStackTrace();
                                    return recordTimestamp;
                                }

                            }
                        })
        );
        //TODO 2.1 .获取ods_base_db主题数据,并转换为bean
            String sourceTopic1 = "dwd_payment_info";
            String group1 = "PaymentWideApp";
            DataStreamSource<String> streamSource1 = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic1,group1));

        SingleOutputStreamOperator<PaymentInfo> paymentInfoDS = streamSource1.map(line ->
        {
            PaymentInfo paymentInfo = JSON.parseObject(line, PaymentInfo.class);
            return paymentInfo;
        }).assignTimestampsAndWatermarks(
                WatermarkStrategy.<PaymentInfo>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                            @Override
                            public long extractTimestamp(PaymentInfo element, long recordTimestamp) {
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
                                String create_time = element.getCreate_time();
                                try {
                                    return sdf.parse(create_time).getTime();
                                } catch (ParseException e) {
                                    e.printStackTrace();
                                    return recordTimestamp;
                                }

                            }
                        }));
                //TODO 3. 双流Interval Join

        SingleOutputStreamOperator<PaymentWide> paymentWide = orderWideDS.keyBy(ow -> ow.getOrder_id())
                .intervalJoin(paymentInfoDS.keyBy(pi -> pi.getOrder_id()))
                .between(Time.seconds(0), Time.minutes(15))
                .process(new ProcessJoinFunction<OrderWide, PaymentInfo, PaymentWide>() {
                    @Override
                    public void processElement(OrderWide ow, PaymentInfo pi, Context ctx, Collector<PaymentWide> out) throws Exception {
                        new PaymentWide(pi, ow);
                    }
                });

        //TODO 4. 将数据写入Kafka
        String sinkTopic = "dwm_payment_wide";
        paymentWide.map(JSON::toJSONString)
                .addSink(MyKafkaUtil.getKafkaProducer(sinkTopic));

        //TODO 5.执行

        env.execute("PaymentWideApp");
    }
}
