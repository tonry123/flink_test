package com.sensorsdata.flink.bilibili;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @description
 * @author: liguangwei
 * @create: 2021-02-27 21:16
 **/
public class EventTimeSessionWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置eventtime为时间标准
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        //1614432007000，spark,3
        //1614432009000,hadoop，5

        SingleOutputStreamOperator<String> lines = env.socketTextStream("10.120.138.222", 8888).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(String line) {
                String[] split = line.split(",");
                return Long.parseLong(split[0]);
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> maped = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String lines) throws Exception {
                String[] split = lines.split(",");
                return Tuple2.of(split[1], Integer.parseInt(split[2]));
            }
        });

        //先分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyByed = maped.keyBy(0);

        //再窗口
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> window = keyByed.window(EventTimeSessionWindows.withGap(Time.seconds(5)));

        SingleOutputStreamOperator<Tuple2<String, Integer>> sumed = window.sum(1);

        sumed.print();
        env.execute("TimeSessionWimdow");
    }
}
