package com.sensorsdata.flink.shangguigu.processFunction;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Tuple;

/**
 * @Author: Li Guangwei
 * @Descriptions: 连续十秒 温度上升
 * @Date: 2021/5/2 22:00
 * @Version: 1.0
 */
public class KeyedProcessFunctionTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socket = env.socketTextStream("xxx", 8888);

        SingleOutputStreamOperator<ReadTmp> map = socket.map(new MapFunction<String, ReadTmp>() {
            @Override
            public ReadTmp map(String s) throws Exception {

                String[] split = s.split(",");

                return new ReadTmp(split[0], new Long(split[1]), new Double(split[2]));

            }
        });

        map.keyBy("name").process(new MyKeyedProcessFunction(10)).print();

        env.execute("xxx");



    }


}
