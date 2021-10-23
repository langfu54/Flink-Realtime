package app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import utils.MyKafkaUtil;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @Author:Langfu54@gmail.com
 * @Date:2021.08
 * @desc:
 */
public class UserJumpDetailApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.创建执行环境，设置并行度
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  //建议和Kafka分区数一致。

        //TODO 开启状态后端
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop101:8020/gmall-flink-210325/ck"));
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);

        //TODO 2. 读取dwd_page_log 主题

        String sourceTopic = "dwd_page_log";
        String group = "UserJumpDetailApp";
        DataStreamSource<String> streamSource = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, group));

        //TODO 3. 将每行数据转换为JSON 并设置WATERMARK
        SingleOutputStreamOperator<JSONObject> jsonObj = streamSource.map(JSON::parseObject).assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                return element.getLong("ts");
                            }
                        })
        );
        //TODO 4. 创建模式序列、
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                String lastPageId = value.getJSONObject("page").getString("last_page_id");
                return lastPageId == null || lastPageId.length() <= 0;
            }
        }).next("next").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                String lastPageId = value.getJSONObject("page").getString("last_page_id");
                return lastPageId == null || lastPageId.length() <= 0;
            }
        }).within(Time.seconds(10));

        //TODO 5. 将模式序列作用到流
        PatternStream<JSONObject> patternStream = CEP
                .pattern(jsonObj.keyBy(json -> json.getJSONObject("common").getString("mid"))
                        , pattern);
        //TODO 6. 提取匹配事件和超时事件
        OutputTag<JSONObject> timeOutTag = new OutputTag<JSONObject>("timeOut") {
        };
        SingleOutputStreamOperator<JSONObject> selectDS = patternStream.select(timeOutTag,
                new PatternTimeoutFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject timeout(Map<String, List<JSONObject>> map, long ts) throws Exception {
                        return map.get("start").get(0);
                    }
                }, new PatternSelectFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject select(Map<String, List<JSONObject>> map) throws Exception {
                        return map.get("start").get(0);
                    }
                });
        DataStream<JSONObject> timeOutDS = selectDS.getSideOutput(timeOutTag);

        //TODO 7. UNION - 转换为单独的一个流，而Connect之后，实际还是单独的两个流
        DataStream<JSONObject> unionDS = selectDS.union(timeOutDS);
        //TODO 8. 写到KAFKA主题
        String sinkTopic = "dwm_user_jump_detail";
        unionDS.map(ds -> ds.toJSONString() )
                .addSink(MyKafkaUtil.getKafkaProducer(sinkTopic));
        //TODO 9. 执行
        env.execute("UserJumpDetailApp");
    }
}
