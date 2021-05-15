package com.sensorsdata.flink.Broadcast.action;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @description
 * @author: liguangwei
 * @create: 2021-02-05 11:53
 **/
public class BroadcastTest {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //准备广播变量数据
        ArrayList<Tuple2<String, Integer>> broadData = new ArrayList<>();
        broadData.add(new Tuple2<>("python", 18));
        broadData.add(new Tuple2<>("scala", 20));
        broadData.add(new Tuple2<>("java", 17));
        DataSource<Tuple2<String, Integer>> dataBroadSource = env.fromCollection(broadData);

        DataSet<Map<String, Integer>> baseData = dataBroadSource.map(new MapFunction<Tuple2<String, Integer>, Map<String, Integer>>() {
            @Override
            public Map<String, Integer> map(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                Map<String, Integer> map = new HashMap<>();
                map.put(stringIntegerTuple2.f0, stringIntegerTuple2.f1);
                return map;
            }
        });
        DataSet<String> dataSource = env.fromElements("python", "java", "java", "kafka", "scala", "redis");

        DataSet<String> result = dataSource.map(new RichMapFunction<String, String>() {
            Map<String, Integer> allMap = new HashMap<String, Integer>();
            List<HashMap<String, Integer>> broadCastMap = new ArrayList<HashMap<String, Integer>>();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                this.broadCastMap = getRuntimeContext().getBroadcastVariable("baseData");
                for (HashMap map : broadCastMap) {
                    allMap.putAll(map);
                }
            }

            @Override
            public String map(String s) throws Exception {
                Integer age = allMap.get(s);
                return s + "," + age;
            }
        }).withBroadcastSet(baseData, "baseData");
        result.print();
    }
}