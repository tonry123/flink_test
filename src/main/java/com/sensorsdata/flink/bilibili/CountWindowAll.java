package com.sensorsdata.flink.bilibili;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.table.expressions.In;
import org.apache.flink.util.Collector;
import scala.Int;

/**
 * @description
 * @author: liguangwei
 * @create: 2021-02-27 09:05
 **/
public class CountWindowAll {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        DataStreamSource<String> xx = env.readTextFile("xx");
//        DataStreamSource<String> xxxx = env.fromElements("xxxx");
        DataStreamSource<String> lines = env.socketTextStream("127.0.0.1", 8888);

//        SingleOutputStreamOperator<Integer> maped = lines.map(new MapFunction<String, Integer>() {
//            @Override
//            public Integer map(String s) throws Exception {
//                return Integer.parseInt(s);
//            }
//        });

        //spark,3
        //shadoop,2
//        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMaped = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
//            @Override
//            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
//                String[] split = s.split(",");
//                collector.collect(new Tuple2<>(split[0], Integer.parseInt(split[1])));
//            }
//        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> maped = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                String[] split = s.split(",");
                return Tuple2.of(split[0], Integer.parseInt(split[1]));
            }
        });

        WindowedStream<Tuple2<String, Integer>, Tuple, GlobalWindow> c = maped.keyBy(0).countWindow(5);
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumed = c.sum(1);
        sumed.print();

        env.execute("countWindowA");
    }
}
