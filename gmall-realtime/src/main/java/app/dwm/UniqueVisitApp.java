package app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import utils.MyKafkaUtil;

import java.text.SimpleDateFormat;

/**
 * @Author:Langfu54@gmail.com
 * @Date:2021.08
 * @desc:
 */
public class UniqueVisitApp {
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
        // TODO 2. 读取Kafka dwd_page_log
        String sourceTopic = "dwd_page_log";
        String group = "UniqueVisitApp";
        DataStreamSource<String> streamSource = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, group));

        // TODO 3. 将数据转换为JSON格式
        SingleOutputStreamOperator<JSONObject> jsonObjDS = streamSource.map(JSON::parseObject);
        // TODO 4.keyby  过滤数据，读取状态，更新状态，设置状态保存TTL 过期时间
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));
        SingleOutputStreamOperator<JSONObject> filteredDS = keyedStream.filter(new RichFilterFunction<JSONObject>() {
            private ValueState<String> dateState;
            private SimpleDateFormat sdf;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> dateStateDesc = new ValueStateDescriptor<String>("date-state", String.class);
                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.hours(24))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                dateStateDesc.enableTimeToLive(ttlConfig);
                dateState = getRuntimeContext().getState(dateStateDesc);


                sdf = new SimpleDateFormat("yyyy-MM-dd");
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {
                String date = sdf.format(value.getLong("ts"));
                String lastPageId = value.getJSONObject("page").getString("last_page_id");
                if (lastPageId == null || lastPageId.length() <= 0) { // lastPageId.length() <= 0 为防止部分情况下，前端数据该值为空只服从
                    String state = dateState.value();
                    if (!date.equals(state)) {
                        dateState.update(date);
                        return true;
                    }
                }
                return false;
            }
        });

        // TODO 5. 写到Kakfa DWM 对应主题
        String topic = "dwm_unique_visit";
        SingleOutputStreamOperator<String> maped = filteredDS.map(JSONAware::toJSONString);
        maped.addSink(MyKafkaUtil.getKafkaProducer(topic));

        maped.print();

        // TODO 6. 执行

        env.execute("UniqueVisitApp");
    }
}
