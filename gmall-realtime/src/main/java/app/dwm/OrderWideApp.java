package app.dwm;

import app.functions.DimAsyncFunction;
import bean.OrderDetail;
import bean.OrderInfo;
import bean.OrderWide;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import utils.DimUtil;
import utils.MyKafkaUtil;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * @Author:Langfu54@gmail.com
 * @Date:2021.08
 * @desc:
 */
public class OrderWideApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.创建执行环境，设置并行度
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //开启状态后端
        // env.setStateBackend(new FsStateBackend("hdfs://hadoop101:8020/gmall-flink-210325/ck"));
        // env.enableCheckpointing(5000L);
        // env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // env.getCheckpointConfig().setCheckpointTimeout(10000L);
        // env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        // env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);
        // TODO 2. 读取Kafka dwd_order_info / dwd_order_detail 转换为JavaBean,并指定WM
        String sourceTopic1 = "dwd_order_info";
        String group = "OrderWideApp";
        DataStreamSource<String> streamSource = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic1, group));
        SingleOutputStreamOperator<OrderInfo> infoStreamwithOutDim = streamSource.map(line ->
                {
                    JSONObject jsonObject = JSON.parseObject(line);
                    String createTime = jsonObject.getString("create_time");
                    String[] split = createTime.split(" ");
                    String create_date = split[0];
                    String create_hour = split[1].split(":")[0];
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
                    long create_ts = sdf.parse(createTime).getTime();

                    jsonObject.put("create_date", create_date);
                    jsonObject.put("create_hour", create_hour);
                    jsonObject.put("create_ts", create_ts);

                    return JSON.parseObject(jsonObject.toJSONString(), OrderInfo.class);
                }
        )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                                    @Override
                                    public long extractTimestamp(OrderInfo element, long recordTimestamp) {
                                        return element.getCreate_ts();
                                    }
                                })
                );

        String sourceTopic2 = "dwd_order_detail";
        String group2 = "OrderWideApp";
        DataStreamSource<String> streamSource2 = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic2, group2));
        streamSource.print();

        SingleOutputStreamOperator<OrderDetail> detailStreamwithOutDim = streamSource2.map(line ->
                {
                    OrderDetail orderDetail = JSON.parseObject(line, OrderDetail.class);
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd dd:hh:ss");

                    String create_time = orderDetail.getCreate_time();
                    long create_ts = sdf.parse(create_time).getTime();
                    orderDetail.setCreate_ts(create_ts);

                    return orderDetail;
                }
        )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderDetail>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                                    @Override
                                    public long extractTimestamp(OrderDetail element, long recordTimestamp) {
                                        return element.getCreate_ts();
                                    }
                                }));

        //TODO 3. 双流JOIN, keyby  interval join

        SingleOutputStreamOperator<OrderWide> processedDS = infoStreamwithOutDim.keyBy(info -> info.getId())
                .intervalJoin(detailStreamwithOutDim.keyBy(detail -> detail.getOrder_id()))
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo info, OrderDetail detail, Context ctx, Collector<OrderWide> out) throws Exception {
                            out.collect(new OrderWide(info,detail));
                    }
                });

        //TODO 4. 关联维度表,查Phoenix数据   //map 是同步方法
//        SingleOutputStreamOperator<OrderWide> orderWidewithDim = processedDS.map(orderwide -> {
//            int dsId = processedDS.getId();
//
//            //TODO 查Phoenix数据，补充OrderWide字段
//            //4.1 补充User信息
//            DimUtil.getDimInfo();
//
//        });
        //应用Async,关联用户维度
        SingleOutputStreamOperator<OrderWide> orderWideWithUserDS = AsyncDataStream.unorderedWait(
                processedDS,
                new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getUser_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                        orderWide.setUser_gender(dimInfo.getString("GENDER"));

                        String birthday = dimInfo.getString("BIRTHDAY");
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

                        long currentTs = System.currentTimeMillis();
                        long ts = sdf.parse(birthday).getTime();

                        long age = (currentTs - ts) / (1000 * 60 * 60 * 24 * 365L);

                        orderWide.setUser_age((int) age);
                    }
                },
                60,
                TimeUnit.SECONDS);

        //打印测试
//        orderWideWithUserDS.print("orderWideWithUserDS");

        //4.2 关联地区维度
        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDS = AsyncDataStream.unorderedWait(orderWideWithUserDS,
                new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getProvince_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                        orderWide.setProvince_name(dimInfo.getString("NAME"));
                        orderWide.setProvince_area_code(dimInfo.getString("AREA_CODE"));
                        orderWide.setProvince_iso_code(dimInfo.getString("ISO_CODE"));
                        orderWide.setProvince_3166_2_code(dimInfo.getString("ISO_3166_2"));
                    }
                }, 60, TimeUnit.SECONDS);

        //4.3 关联SKU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSkuDS = AsyncDataStream.unorderedWait(
                orderWideWithProvinceDS, new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setSku_name(jsonObject.getString("SKU_NAME"));
                        orderWide.setCategory3_id(jsonObject.getLong("CATEGORY3_ID"));
                        orderWide.setSpu_id(jsonObject.getLong("SPU_ID"));
                        orderWide.setTm_id(jsonObject.getLong("TM_ID"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSku_id());
                    }
                }, 60, TimeUnit.SECONDS);

        //4.4 关联SPU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSpuDS = AsyncDataStream.unorderedWait(
                orderWideWithSkuDS, new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setSpu_name(jsonObject.getString("SPU_NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSpu_id());
                    }
                }, 60, TimeUnit.SECONDS);

        //4.5 关联TM维度
        SingleOutputStreamOperator<OrderWide> orderWideWithTmDS = AsyncDataStream.unorderedWait(
                orderWideWithSpuDS, new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setTm_name(jsonObject.getString("TM_NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getTm_id());
                    }
                }, 60, TimeUnit.SECONDS);

        //4.6 关联Category维度
        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3DS = AsyncDataStream.unorderedWait(
                orderWideWithTmDS, new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setCategory3_name(jsonObject.getString("NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getCategory3_id());
                    }
                }, 60, TimeUnit.SECONDS);

        orderWideWithCategory3DS.print("orderWideWithCategory3DS>>>>>>>>>>>");


        //TODO 5. 数据存入Kafka
        String sinkTopic = "dwm_order_wide";
        orderWideWithCategory3DS.map(JSON::toJSONString)
                .addSink(MyKafkaUtil.getKafkaProducer(sinkTopic));

        //TODO 6. 执行
        env.execute("OrderWideApp");
    }
}
