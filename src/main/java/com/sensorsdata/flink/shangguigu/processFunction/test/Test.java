package com.sensorsdata.flink.shangguigu.processFunction.test;

import com.alibaba.fastjson.JSONObject;
import com.sensorsdata.flink.Pojo;
import com.sensorsdata.flink.SQL.MaxMin;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * @description  找出最大值和最小值
 * @author: liguangwei
 * @create: 2021-02-04 10:48
 **/
public class Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> data = env.fromElements(8,1, 2, 3, 4, 5, 0, -2, 4, 3);


        SingleOutputStreamOperator<Pojo> aa = data.map(line -> {
            return new Pojo("a", new Integer(line));
        });

        aa.keyBy("t1").process(new Myfunction()).print();

        env.execute("zz");

    }

    public static class MyMaxTup implements ReduceFunction<Pojo>{

        @Override
        public Pojo reduce(Pojo pojo, Pojo t1) throws Exception {
            return pojo.getT2() > t1.getT2() ? pojo : t1;
        }
    }

    public static class MyMaxTup2 implements ReduceFunction<Pojo>{

        @Override
        public Pojo reduce(Pojo pojo, Pojo t1) throws Exception {
            return pojo.getT2() < t1.getT2() ? pojo : t1;
        }
    }

    public static class Myfunction extends KeyedProcessFunction<Tuple, Pojo, MaxMin>{
        private transient ReducingState reducingState;
        private transient ReducingState reducingState2;
        @Override
        public void open(Configuration parameters) throws Exception {
            ReducingStateDescriptor reducingStateDescriptor = new ReducingStateDescriptor("max-min",new MyMaxTup(),Pojo.class);

            ReducingStateDescriptor reducingStateDescriptor2 = new ReducingStateDescriptor("max-min2",new MyMaxTup2(),Pojo.class);

            reducingState = getRuntimeContext().getReducingState(reducingStateDescriptor);
            reducingState2 = getRuntimeContext().getReducingState(reducingStateDescriptor2);
        }

        @Override
        public void processElement(Pojo pojo, Context context, Collector<MaxMin> collector) throws Exception {
            reducingState.add(pojo);
            reducingState2.add(pojo);
            collector.collect(new MaxMin(((Pojo)reducingState.get()).getT2(),((Pojo)reducingState2.get()).getT2()));
        }



    }
}
