package com.sensorsdata.flink.shangguigu.processFunction.test;

import com.sensorsdata.flink.shangguigu.processFunction.test.beans.PV;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: Li Guangwei
 * @Descriptions: <a,2>,<a,5>,<b,2>,<b,2>,<b,2>, 统计最大的的结果为<a,7>
 * @Date: 2021/5/11 11:51
 * @Version: 1.0
 */
public class Test2 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> datas = env.fromElements("a,2", "a,3", "b,4", "c,2", "c,4", "b,1");

        SingleOutputStreamOperator<PV> mapSource = datas.map(line -> {
            String[] s = line.split(",");
            return new PV(s[0], Long.valueOf(s[1]));
        });

        mapSource.keyBy(a)
    }
}
