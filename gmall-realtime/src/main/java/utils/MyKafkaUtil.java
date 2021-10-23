package utils;

import org.apache.flink.api.common.serialization.*;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

/**
 * @Author:Langfu54@gmail.com
 * @Date:2021.08
 * @desc:
 */
public class MyKafkaUtil {

    private static String brokers = "hadoop101:9092,hadoop102:9092,hadoop103:9092";
    private static String defautl_topic = "default_topic";
    public static String KafkaConstant(String topic,String group) {
      return   "  'connector' = 'kafka', " +
                "  'topic' = '" + topic + "', " +
                "  'properties.bootstrap.servers' = 'hadoop101:9092', " +
                "  'properties.group.id' = '" + group + "', " +
                "  'scan.startup.mode' = 'latest-offset', " +
                "  'format' = 'json' " +
                ")";
    }


    /**
     * which provide a generic function for user to define, user shoud define KafkaSerializationSchema
     * @param kafkaSerializationSchema
     * @param <T>
     * @return
     */
    public static <T> FlinkKafkaProducer<T> GenericKafkaProducer(KafkaSerializationSchema<T> kafkaSerializationSchema){
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,brokers);
        return new FlinkKafkaProducer<T>(
                defautl_topic,
                kafkaSerializationSchema,
                properties,
                FlinkKafkaProducer.Semantic.NONE
        );
    }

    //kafka producer
    public static FlinkKafkaProducer<String> getKafkaProducer(String topic){
      return new FlinkKafkaProducer<String>(
              brokers,
              topic,
              new SimpleStringSchema()
        );
    }

    //kafka consumer
    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic,String cnsumerGroup){
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,brokers);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,cnsumerGroup);
        return new FlinkKafkaConsumer<String>(
                topic,
                new SimpleStringSchema(),
                properties
        );
    }
}
