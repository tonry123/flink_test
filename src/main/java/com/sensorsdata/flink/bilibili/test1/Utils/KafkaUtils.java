package com.sensorsdata.flink.bilibili.test1.Utils;

import com.sensorsdata.flink.kafka.utils.Constants;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;

import java.util.Properties;

/**
 * @description
 * @author: liguangwei
 * @create: 2021-02-28 17:41
 **/
public class KafkaUtils {

    public static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    public static DataStreamSource<String> kafkaFunction( SimpleStringSchema simpleStringSchema){

        Properties props = new Properties();
        props.put("bootstrap.servers", Constants.KAFKA_BROKERS);
        props.put("group.id", Constants.KAFKA_GROUP_ID);
        //不要自动提交偏移量，让flink交给checkpoint管理偏移量。
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        DataStreamSource<String> source = env.addSource(new FlinkKafkaConsumer010<>(Constants.KAFKA_TOPIC, simpleStringSchema, props));

        return source;
    }

    public static StreamExecutionEnvironment getEnv(){
        return env;
    }

}
