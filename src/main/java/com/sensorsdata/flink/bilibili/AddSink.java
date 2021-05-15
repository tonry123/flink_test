package com.sensorsdata.flink.bilibili;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;

/**
 * @description
 * @author: liguangwei
 * @create: 2021-02-21 12:32
 **/
public class AddSink {
    public static void main(String[] args) throws Exception {
        //创建flink stream 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //socket
        DataStreamSource<String> socket = env.socketTextStream("10.120.138.222", 8889);

        //transformation
//        SingleOutputStreamOperator<String> words = socket.flatMap(new FlatMapFunction<String, String>() {
//            @Override
//            public void flatMap(String s, Collector<String> collector) throws Exception {
//                String[] words = s.split(" ");
//                for (String word : words) {
//                    collector.collect(word);
//                }
//            }
//        });
//
//        SingleOutputStreamOperator<Tuple2<String, Integer>> wordOne = words.map(new MapFunction<String, Tuple2<String, Integer>>() {
//            @Override
//            public Tuple2<String, Integer> map(String s) throws Exception {
//                return Tuple2.of(s, 1);
//            }
//        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordOne = socket.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    Tuple2<String, Integer> tuple = Tuple2.of(word, 1);
                    collector.collect(tuple);

                }
            }
        });

        //从该算子开启一个新链，发生redistributin
        //wordOne.startNewChain();
        //从该算子开始到该算子结束，算一个task，跟其他算子不再operator chain
        wordOne.disableChaining();


        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = wordOne.keyBy(0).sum(1);
        sum.addSink(new RichSinkFunction<Tuple2<String, Integer>>() {



            @Override
            public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
                int index = getRuntimeContext().getIndexOfThisSubtask();
                System.out.println(index + 1 +" > "+value);
            }
        });


        env.execute("StreamWordCount");


    }
}
