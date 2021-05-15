package com.sensorsdata.flink.bilibili.test1;

import com.sensorsdata.flink.bilibili.test1.Utils.KafkaUtils;
import com.sensorsdata.flink.bilibili.test1.bean.Order;
import com.sensorsdata.flink.kafka.utils.Constants;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @description
 * @author: liguangwei
 * @create: 2021-02-28 17:40
 **/
public class TestRichMapFunction {
    public static void main(String[] args) throws Exception {

        DataStreamSource<String> stringDataStreamSource = KafkaUtils.kafkaFunction(new SimpleStringSchema());

        SingleOutputStreamOperator<Order> maped = stringDataStreamSource.map(new Date2Bean());

        maped.print();

        KafkaUtils.getEnv().execute("TestRichMapFunction");


    }
}
