package com.sensorsdata.flink.RichMapFunction.action;

import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


import javax.security.auth.login.Configuration;

/**
 * @description
 * @author: liguangwei
 * @create: 2021-02-04 10:56
 **/
public class RichFuntion {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator source=env.readTextFile("/Users/apple/Desktop/上海/testFile/flinkdate.txt").map(new RichMapFunction<String, Object>() {
            @Override
            public Object map(String s) throws Exception {
                return "------";
            }

            public void open(Configuration parameters) throws Exception {
                System.out.println("======");
            }

        });


        source.print();


        env.execute("jahahaha");


    }
}








