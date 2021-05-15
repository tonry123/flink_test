package com.sensorsdata.flink.kafka.utils;

public class Constants {

    public static final String KAFKA_BROKERS = ConfigUtils.getInstanceNew().get("kafka.brokers");
    public static final String KAFKA_GROUP_ID = ConfigUtils.getInstanceNew().get("kafka.groupid");
    public static final String KAFKA_TOPIC = ConfigUtils.getInstanceNew().get("kafka.topic");
    public static final Integer KAFKA_CONSUMER_NUM = ConfigUtils.getInstanceNew().getInteger("kafka.consumer.num");
    public static final String SA_CONSUMER_LOG = ConfigUtils.getInstanceNew().get("sa.comsumer.log");
    public static final String KAFKA_OFFSET_RESET = ConfigUtils.getInstanceNew().get("kafka.offset.reset");




}
