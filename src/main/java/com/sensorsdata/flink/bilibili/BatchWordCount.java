package com.sensorsdata.flink.bilibili;




import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @description
 * @author: liguangwei
 * @create: 2021-02-21 16:21
 **/
public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        //离线批处理 少了stream
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> lines = env.readTextFile(args[0]);

        FlatMapOperator<String, Tuple2<String, Integer>> word = lines.flatMap((String line, Collector<Tuple2<String, Integer>> out) -> {
            Arrays.stream(line.split(" ")).forEach(w -> {
                out.collect(Tuple2.of(w, 1));
            });
        }).returns(Types.TUPLE(Types.STRING, Types.INT));

        AggregateOperator<Tuple2<String, Integer>> sumed = word.groupBy(0).sum(1);

        sumed.print();


    }
}
