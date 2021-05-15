package com.sensorsdata.flink.bilibili.Sink;

import com.sensorsdata.flink.bilibili.test1.Utils.KafkaUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;

/**
 * @description
 * @author: liguangwei
 * @create: 2021-03-07 12:07
 **/
public class Main {
    public static void main(String[] args) throws Exception {
        DataStreamSource<String> stringDataStreamSource = KafkaUtils.kafkaFunction(new SimpleStringSchema());

        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("10.120.138.222").setPort(6178).setPassword("SensorsData2015").build();

        DataStreamSink<String> stringDataStreamSink = stringDataStreamSource.addSink(new RedisSink<String>(conf, new RedisSinkTest()));

        KafkaUtils.getEnv().execute("test");



    }
}
