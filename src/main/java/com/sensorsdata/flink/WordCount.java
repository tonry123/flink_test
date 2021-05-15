package com.sensorsdata.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.util.Collector;

public class WordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = environment.socketTextStream("10.120.138.222", 8888);
        SingleOutputStreamOperator<Tuple2<String, Integer>> map1 = source.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });
//        SingleOutputStreamOperator<Tuple2<String, Integer>> map2 = source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
//            @Override
//            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
//                String[] split = s.split(",");
//                for (String word : split) {
//                    collector.collect(new Tuple2<>(word, 1));
//                }
//            }
//        });



//        SingleOutputStreamOperator<Tuple2<String, Integer>> sum1 = map1.keyBy(0).sum(1);
//        SingleOutputStreamOperator<Tuple2<String, Integer>> sum2 = map2.keyBy(0).sum(1);
//        DataStreamSink<Tuple2<String, Integer>> tuple2DataStreamSink = sum1.addSink(new SinkFunction<Tuple2<String, Integer>>() {
//            @Override
//            public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
//
//            }
//        });

        source.writeToSocket("10.120.138.222", 8889, new SerializationSchema<String>() {
            @Override
            public byte[] serialize(String s) {
                byte[] bytes = s.getBytes();
                return bytes;
            }
        });
        environment.execute("word count");

    }
}
