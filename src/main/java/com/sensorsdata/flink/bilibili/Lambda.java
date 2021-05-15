package com.sensorsdata.flink.bilibili;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.lang.reflect.Type;
import java.util.Arrays;

/**
 * @description
 * @author: liguangwei
 * @create: 2021-02-21 15:42
 **/
public class Lambda {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StateTtlConfig.newBuilder(Time.seconds(1)).setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired).build();

        DataStreamSource<String> lines = env.socketTextStream("10.120.138.222", 8889);


        //匿名实现类
//        SingleOutputStreamOperator<Tuple2<String, Integer>> word = lines.flatMap((String line, Collector<Tuple2<String, Integer>> out) -> {
//            Arrays.stream(line.split(" ")).forEach(w -> {
//                out.collect(Tuple2.of(w, 1));
//            });
//        }).returns(Types.TUPLE(Types.STRING, Types.INT));


        //lambda 2
        SingleOutputStreamOperator<String> words = lines.flatMap((String line, Collector<String> out) -> {
            Arrays.stream(line.split(" ")).forEach(out::collect);
        }).returns(Types.STRING);




        SingleOutputStreamOperator<Tuple2<String, Integer>> word = words.map(w -> Tuple2.of(w, 1)).returns(Types.TUPLE(Types.STRING, Types.INT));
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumed = word.keyBy(0).sum(1);
        sumed.print();

        env.execute("Lambda");
    }
}
