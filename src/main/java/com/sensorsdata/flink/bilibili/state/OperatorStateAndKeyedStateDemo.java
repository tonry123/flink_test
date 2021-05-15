package com.sensorsdata.flink.bilibili.state;

import com.sensorsdata.flink.kafka.action.FlinkConsumerKakfa;
import com.sensorsdata.flink.kafka.utils.Constants;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

/**
 * @description
 * @author: liguangwei
 * @create: 2021-03-07 21:27
 **/
public class OperatorStateAndKeyedStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //checkpoint 间隔为2s
        env.enableCheckpointing(2000);

        //重启策略额
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5,3000));

        //backend
        env.setStateBackend(new FsStateBackend(args[0]));

        //默认不保存。(DELETE_ON_CANCELLATION).如果需要修改程序，下次重新部署，保存上次的state 开启在 job 中止后仍然保留的 externalized checkpoints
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //实现exactly_once
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);


        //kafka
        Properties props = new Properties();
        props.put("bootstrap.servers", Constants.KAFKA_BROKERS);
        props.put("group.id", Constants.KAFKA_GROUP_ID);
        //不要自动提交偏移量，让flink交给checkpoint管理偏移量。
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        DataStreamSource<String> lines = env.addSource(new FlinkKafkaConsumer010<>("lgw1", new SimpleStringSchema(), props));


        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMaped = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] spl = s.split(" ");

                for (String str : spl) {
                    collector.collect(Tuple2.of(str, 1));
                }
            }
        }).uid("OperatoredStateUid");

        SingleOutputStreamOperator<Tuple2<String, Integer>> sumed = flatMaped.keyBy(0).sum(1).uid("keyedStateUid");

        sumed.print();

        env.execute("OperatorStateAndKeyedStateDemo");
    }
}
