package app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import utils.MyKafkaUtil;

/**
 * @Author:Langfu54@gmail.com
 * @Date:2021.08
 * @desc:
 */
public class BaseLogAppTest {
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
        //2.定义flinkCDC Source,从Kafka指定主题获取数据
        String sourceTopic = "ods_base_log";
        String group = "dwd_base_log";
        DataStreamSource<String> streamSource = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic,group));
        //将数据转换为JSON，并将脏数据在侧输出流打印
        OutputTag<String> dirtyTag = new OutputTag<String>("dirty"){};
        SingleOutputStreamOperator<JSONObject> JsonStream = streamSource.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    e.printStackTrace();
                    ctx.output(dirtyTag, value);
                }
            }
        });
        //将脏数据做打印输出
        System.out.println("》》》》》Dirty");
        JsonStream.getSideOutput(dirtyTag).print();

        KeyedStream<JSONObject, String> keyedStream = JsonStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getJSONObject("common").getString("mid");
            }
        });

        //对is_new 字段进行校验
        SingleOutputStreamOperator<JSONObject> chekedIsNew = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            private ValueState<String> newState;

            //每个并行实例只执行一次 , 不同KEY在一个分区？？
            @Override
            public void open(Configuration parameters) throws Exception {
                ValueState<String> newState = getRuntimeContext().getState(new ValueStateDescriptor<String>("is_new", String.class));
            }

            @Override

            public JSONObject map(JSONObject value) throws Exception {
                String isnew = value.getJSONObject("common").getString("is_new");
                if ("1".equals(isnew)) {
                    String state = newState.value();
                    if (state != null) { //说明数据存在，需要修改为0
                        value.getJSONObject("common").put("is_new", "0");
                    } else {
                        newState.update("1");   //如何设置24为新用户
                    }
                }
                return value;
            }
        });

        //将不同数据发到不同的流,侧输出流，需要用process（）
        OutputTag<String> startTag = new OutputTag<String>("start") {
        };
        OutputTag<String> displayTag = new OutputTag<String>("display") {
        };
        //TODO .分流  侧输出流  页面：主流  启动：侧输出流  曝光：侧输出流

        SingleOutputStreamOperator<String> seperateStream = chekedIsNew.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {
                //判断启动
                JSONObject startLog = value.getJSONObject("start");
                if (startLog != null && startLog.size() > 0) {
                    ctx.output(startTag, value.toJSONString());
                } else {
                    out.collect(value.toJSONString());
                    JSONArray displays = value.getJSONArray("displays");

                    if (displays != null && displays.size() > 0) {//why >0
                        String page = value.getJSONObject("page").getString("page_id");
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            display.put("page_id", page);
                            ctx.output(displayTag, display.toJSONString());
                        }
                    }
                }
            }
        });

        DataStream<String> dis = seperateStream.getSideOutput(displayTag);
        DataStream<String> start = seperateStream.getSideOutput(startTag);


        //可以选择打印

        //写到Kafka

        seperateStream.addSink(MyKafkaUtil.getKafkaProducer("dwd_start_log"));
        seperateStream.addSink(MyKafkaUtil.getKafkaProducer("dwd_page_log"));
        seperateStream.addSink(MyKafkaUtil.getKafkaProducer("dwd_display_log"));

        //4.执行Env,并为Job指定名称
        env.execute("baseLogApp");
    }
}
