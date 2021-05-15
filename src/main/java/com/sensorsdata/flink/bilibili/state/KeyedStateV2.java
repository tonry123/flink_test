package com.sensorsdata.flink.bilibili.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.expressions.In;
import org.apache.flink.util.Collector;

/**
 * @description
 * @author: liguangwei
 * @create: 2021-03-09 10:14
 **/
public class KeyedStateV2 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
        //重启策略额
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5,3000));

        DataStreamSource<String> lines = env.socketTextStream("10.120.138.222", 8888);

        SingleOutputStreamOperator<String> flatmaped = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] sp = s.split(" ");

                for (String str : sp) {
                    collector.collect(str);
                }
            }
        });
        SingleOutputStreamOperator<Tuple2<String, Integer>> maped = flatmaped.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                if(s.startsWith("hello")){
                    System.out.println(1/0);
                }
                return Tuple2.of(s, 1);
            }
        });

        KeyedStream<Tuple2<String, Integer>, Tuple> tuple2TupleKeyedStream = maped.keyBy(0);


        SingleOutputStreamOperator<Tuple2<String, Integer>> keyedStreamed = tuple2TupleKeyedStream.map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {

            //定义状态
            private transient ValueState<Integer> valueState;
            @Override
            public void open(Configuration parameters) throws Exception {

                //定义一个状态啊描述器
                ValueStateDescriptor<Integer> valueStateDescriptor = new ValueStateDescriptor<Integer>(
                        "wc-keyed-state",
//                        TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {})
                        Types.INT
                );
                valueState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                String word = stringIntegerTuple2.f0;
                Integer count = stringIntegerTuple2.f1;
                Integer historyKyed = valueState.value();
                if(historyKyed != null){
                    System.out.println("ss----------");
                    historyKyed += count;
                    valueState.update(historyKyed);
                    return Tuple2.of(word, historyKyed);
                }else {
                    System.out.println("=============");
                    valueState.update(count);
                    return stringIntegerTuple2;
                }

            }
        });


        keyedStreamed.print();
        env.execute("KeyedStateV2");
    }
}
