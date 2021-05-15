package com.sensorsdata.flink.SQL.action;

import com.sensorsdata.flink.SQL.model.Students;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;


import org.apache.flink.runtime.topology.Result;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import scala.Int;

/**
 * @description
 * @author: liguangwei
 * @create: 2021-02-03 16:01
 **/
public class Sql {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);
//        BatchTableEnvironment tableEnv = null;

        DataSet<Students> input = env.readTextFile("/Users/apple/Desktop/上海/testFile/flinkdate.txt")
                .map(value->{
                    String[] split = value.split(";");
                    if(split.length == 2){
                        return new Students(split[0], Integer.parseInt(split[1]));
                    }
                    return null;
                });



        Table topAge = tableEnv.fromDataSet(input);
        tableEnv.registerTable("agetable",topAge);
        Table queryResult = tableEnv.sqlQuery("select name,age  from agetable order by age asc ");
        DataSet<Students> result = tableEnv.toDataSet(queryResult, Students.class);
        result.print();
//        env.execute("ssss");
    }
}




