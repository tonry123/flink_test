package com.sensorsdata.flink.shangguigu.processFunction;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;


/**
 * @Author: Li Guangwei
 * @Descriptions: TODO
 * @Date: 2021/5/2 23:26
 * @Version: 1.0
 */
public class MyKeyedProcessFunction extends KeyedProcessFunction<Tuple, ReadTmp, String> {
    private int interval;
    public MyKeyedProcessFunction(Integer interval){
        this.interval = interval;
    }

    //保存上一次温度值
    private ValueState<Double> lastTmpState;
    private ValueState<Long> timerTsState;

    @Override
    public void open(Configuration parameters) throws Exception {

        lastTmpState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("lastTmpState",Double.class, Double.MIN_VALUE));

        timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerTsState",Long.class));


    }

    @Override
    public void processElement(ReadTmp readTmp, Context context, Collector<String> collector) throws Exception {
        Double lastTmp = lastTmpState.value();
        Long timerTs = timerTsState.value();

        //如果后续温度上升，并且没有定时器，就注册定时器
        if(readTmp.getTmp() > lastTmp && timerTs == null){
            //注册定时器
            Long ts = context.timerService().currentProcessingTime() + interval * 1000L;
            context.timerService().registerEventTimeTimer(ts);
            timerTsState.update(ts);

        }
        //如果温度下降，就删除定时器
        else if ( readTmp.getTmp() < lastTmp && timerTs != null){
            context.timerService().deleteEventTimeTimer(timerTs);
            timerTsState.clear();
        }

        //更新状态
        lastTmpState.update(lastTmp);


    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        out.collect("传感器："+ ctx.getCurrentKey()+"连续十秒上升！！！");
        timerTsState.clear();
    }


    @Override
    public void close() throws Exception {
        lastTmpState.clear();
    }
}
