package com.sensorsdata.flink.kafka.utils;

import java.util.Properties;

/**
 * @description
 * @author: liguangwei
 * @create: 2021-02-02 17:11
 **/
public class ConsumerProperties {
    public static Properties getConsumerProperties(){
        Properties props = new Properties();
        props.put("bootstrap.servers", Constants.KAFKA_BROKERS);
        props.put("group.id", Constants.KAFKA_GROUP_ID);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("auto.offset.reset", Constants.KAFKA_OFFSET_RESET);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }
}
