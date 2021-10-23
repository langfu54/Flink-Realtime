package app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import utils.MyKafkaUtil;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author:Langfu54@gmail.com
 * @Date:2021.08
 * @desc:
 */
public class BaseLogApp {
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


        //对数据进行划分为三个主题 dwd_page_log(display 同属于该主题，其它非start的也归属于该主题) / dwd_start_log / dwd_display_log
        //3.1 将获取到的数据转换为JSON 格式,但是会有脏数据，使用侧输出流输出
        OutputTag<String> dirty = new OutputTag<String>("dirty"){};
        SingleOutputStreamOperator<JSONObject> jsonObj = streamSource.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    //发生异常,将数据写入侧输出流
                    ctx.output(dirty, value);
                }
            }
        });

        //3.2.按照Mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObj.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getJSONObject("common").getString("mid");
            }
        });

        //3.2 使用键控状态，修改Common信息的is_new字段
        SingleOutputStreamOperator<JSONObject> isNewCheckedStream = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            private ValueState<String> valueState;

            @Override
            //初始化方法，每个并行度只调用一次
            public void open(Configuration parameters) throws Exception {
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("value-state", String.class));
            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {
                String is_new = value.getJSONObject("common").getString("is_new");
                if ("1".equals(is_new)) {
                    //获取状态数据
                    String state = valueState.value();

                    if (state != null) { //该key状态存在
                        //修改isNew标记
                        value.getJSONObject("common").put("is_new", "0");
                        //TODO 为提高性能，在此处清除该Key的状态？
                    } else {
                        valueState.update("1");
                    }
                }
                return value;
            }
        });

        //根据数据类型，发到不同的侧输出流
        OutputTag<String> startTag = new OutputTag<String>("start"){};
        OutputTag<String> displayTag = new OutputTag<String>("display"){};

        SingleOutputStreamOperator<String> splittoEachStream = isNewCheckedStream.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {
                JSONObject startPage = value.getJSONObject("start");
                if (startPage != null && startPage.size()>0 ) {
                    ctx.output(startTag, value.toJSONString());
                } else { //除start，其余归于page，包括display
                    //将其写入主流
                    out.collect(value.toJSONString());
                    //取出数据中的曝光数据
                    JSONArray displays = value.getJSONArray("displays");
                    if (displays != null && displays.size() > 0) {
                        //获取页面ID
                        String pageId = value.getJSONObject("page").getString("page_id");

                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            //添加页面id
                            display.put("page_id", pageId);
                            //将输出写出到曝光侧输出流
                            ctx.output(displayTag, display.toJSONString());
                        }
                    }
                }
            }
        });

        //dwd_page_log(display 同属于该主题，其它非start的也归属于该主题) / dwd_start_log / dwd_display_log

        // TODO 获取侧输出流，并将数据写入到Kafka
        DataStream<String> displaysideOutput = splittoEachStream.getSideOutput(displayTag);
        DataStream<String> startsideOutput = splittoEachStream.getSideOutput(startTag);

        String startTopic = "dwd_start_log";
        String displayTopic = "dwd_display_log";
        String pageTopic = "dwd_page_log";

        startsideOutput.addSink(MyKafkaUtil.getKafkaProducer(startTopic));
        displaysideOutput.addSink(MyKafkaUtil.getKafkaProducer(displayTopic));
        splittoEachStream.addSink(MyKafkaUtil.getKafkaProducer(pageTopic));

        //4.执行Env,并为Job指定名称
        env.execute("baseLogApp");
    }
}
