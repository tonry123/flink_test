package com.sensorsdata.flink.bilibili.checkpoint;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @description
 * @author: liguangwei
 * @create: 2021-03-07 13:00
 **/
public class localFile {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        //checkpoint
        env.enableCheckpointing(1000);

        //设置重启策略。隔两2秒启动，最多重启5次
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(3,2000));

        //backend
        env.setStateBackend(new FsStateBackend("/Users/apple/Desktop/上海/FlinkD/data/backend"));



        DataStreamSource<String> socket = env.socketTextStream("10.120.138.222", 8888);

        SingleOutputStreamOperator<Tuple2<String, Integer>> maped = socket.map(new MapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                if (s.startsWith("hello")) {
                    throw new Exception("程序挂了");
                }
                return Tuple2.of(s,1);
            }
        });
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumed = maped.keyBy(0).sum(1);

        sumed.print();

        env.execute("test");
    }
}
