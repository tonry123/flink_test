package com.sensorsdata.flink.bilibili.checkpoint;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.hdfs.server.namenode.CheckpointConf;
import scala.reflect.internal.Trees;

/**
 * @description
 * @author: liguangwei
 * @create: 2021-03-07 13:00
 **/
public class hdfs {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //如果是本地运行，需要写这行代码
//        System.setProperty("HADOOP_USER_NAME","sa_cluster");
        //checkpoint 每 1000ms 开始一次 checkpoint
        env.enableCheckpointing(1000);

        //设置重启策略。隔两秒启动，最多重启三次
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(3,2000));

        //backend
        env.setStateBackend(new FsStateBackend(args[0]));

        //默认不保存。(DELETE_ON_CANCELLATION).如果需要修改程序，下次重新部署，保存上次的state 开启在 job 中止后仍然保留的 externalized checkpoints
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        DataStreamSource<String> socket = env.socketTextStream(args[1], 8888);

        SingleOutputStreamOperator<Tuple2<String, Integer>> maped = socket.map(new MapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                if (s.startsWith("hello")) {
                    throw new Exception("程序挂了");
                }
                return Tuple2.of(s,1);
            }
        });
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumed = maped.keyBy(0).sum(1);

        sumed.print();

        env.execute("test");
    }
}
