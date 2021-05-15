package com.sensorsdata.flink.bilibili;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @description
 * @author: liguangwei
 * @create: 2021-02-21 20:55
 **/
public class KeyBy {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStreamSource<String> lines = env.socketTextStream("10.120.138.222", 8889);

        SingleOutputStreamOperator<Tuple3<String, String, Double>> provinceCityAndMoney = lines.map(new MapFunction<String, Tuple3<String, String, Double>>() {
            @Override
            public Tuple3<String, String, Double> map(String s) throws Exception {
                String[] split = s.split(" ");
                String province = split[0];
                String city = split[1];
                double money = Double.parseDouble(split[2]);
                return Tuple3.of(province, city, money);

            }
        });

        SingleOutputStreamOperator<Tuple3<String, String, Double>> keyed = provinceCityAndMoney.keyBy(0, 1).sum(2);

        keyed.print();

        env.execute("KeyBy");
    }
}
