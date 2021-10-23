package app.dwd;

import app.functions.CustomDeserilizer;
import app.functions.DimSinkFunction;
import app.functions.TableProcessFunction;
import bean.TableProcess;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;
import utils.MyKafkaUtil;

import javax.annotation.Nullable;

/**
 * @Author:Langfu54@gmail.com
 * @Date:2021.08
 * @desc:
 */
public class BaseDBApp {
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
        //TODO 2.获取ods_base_db主题数据
        String sourceTopic = "ods_base_db";
        String group = "BaseDBApp";
        DataStreamSource<String> streamSource = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic,group));

        //TODO 3.将每行数据转换为JSON对象并过滤(delete) 主流
        SingleOutputStreamOperator<JSONObject> jsonObj = streamSource.map(line -> JSON.parseObject(line));
            //TODO 3.1 过滤delete数据
//        SingleOutputStreamOperator<JSONObject> filteredDstream = jsonObj.filter(new FilterFunction<JSONObject>() {
//            @Override
//            public boolean filter(JSONObject value) throws Exception {
//                if ("delete".equals(value.getString("type"))) {
//                    return false;
//                }
//                return true;
//            }
//        });
        SingleOutputStreamOperator<JSONObject> filteredDstream = jsonObj.filter((FilterFunction<JSONObject>) value -> ! "delete".equals(value.getString("type")));

        //TODO 4.FlinkCDC 消费MYSQL配置表数据，处理为广播流
        /*
                table_process字段
            sourceTable  	type  	sinkType  sinkTable      sinkColumns pk extend
            base_trademark	insert	hbase     dim_xxx(表名)
            order_info		insert  kafka     dwd_xxa(主题名)
            order_info		update  kafka     dwd_xxb(主题名)
         */
        DebeziumSourceFunction<String> CDCSource = MySQLSource.<String>builder()  //泛型方法
                .hostname("hadoop101")
                .port(3306)
                .username("root")
                .password("1234")
                .databaseList("gmall2021-realtime")
                .deserializer(new CustomDeserilizer())
                .startupOptions(StartupOptions.initial())
                .build();

        DataStreamSource<String> cdcSource = env.addSource(CDCSource);
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = cdcSource.broadcast(mapStateDescriptor);
        //print the cdc
        cdcSource.print();

        //TODO 5.Connect 主流和广播流
        BroadcastConnectedStream<JSONObject, String> connectedStream = filteredDstream.connect(broadcastStream);

        //TODO 6.分别处理主流和广播流  - ?
        /*
        广播流：
            1.解析数据  String => TableProcess
            2.检查HBASE表是否存在并建表
            3.写入状态
        主流：
            1.读取状态
            2.过滤数据
            3.分流
         */
        //TODO 构建测输出流
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>("hbase" ) {};

        SingleOutputStreamOperator<JSONObject> processedDSteream = connectedStream.process(new TableProcessFunction(hbaseTag,mapStateDescriptor));

        //TODO 7.提取侧输出流数据
        DataStream<JSONObject> hbasesideOutput = processedDSteream.getSideOutput(hbaseTag);

        //TODO 8.将Kafka数据写入Kafka主题,
        processedDSteream.addSink(MyKafkaUtil.GenericKafkaProducer(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObject, @Nullable Long aLong) {
                String sinkTable = jsonObject.getString("sinkTable");
                return new ProducerRecord<byte[], byte[]>(sinkTable,
                        jsonObject.getString("after").getBytes());
            }
        }));

        //TODO 将HBase数据写入Phoenix表
        processedDSteream.print();
        hbasesideOutput.addSink(new DimSinkFunction());

        //TODO 9.执行
        env.execute("BaseDBApp");
    }
}
