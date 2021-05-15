package com.sensorsdata.flink.shangguigu.processFunction.shizhan.hotItme;

import com.google.common.collect.Lists;
import com.sensorsdata.flink.shangguigu.processFunction.shizhan.hotItme.beans.ItemViewCount;
import com.sensorsdata.flink.shangguigu.processFunction.shizhan.hotItme.beans.UserBehavior;
import org.apache.commons.net.ntp.TimeStamp;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;


/**
 * @Author: Li Guangwei
 * @Descriptions: TODO
 * @Date: 2021/5/10 22:07
 * @Version: 1.0
 */
public class HotItem {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> inputStream = env.readTextFile("");

        //转换poojo，分配时间戳和watermark
        SingleOutputStreamOperator<UserBehavior> dataStream = inputStream.map(line -> {
            String[] fileds = line.split(",");
            return new UserBehavior(new Long(fileds[0]), new Long(fileds[1]), new Integer(fileds[2]), fileds[3], new Long(fileds[4]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior userBehavior) {
                //由于该字段是秒时间戳，转化为毫秒
                return userBehavior.getTimestamp() * 1000L;
            }
        });

        //分组开窗口，得到每个商品的count
        DataStream<ItemViewCount> windowAggStream = dataStream
                .filter(data -> "pv".equals(data.getBehavior())) //过滤pv
                .keyBy("itemId")
                .timeWindow(Time.hours(1), Time.seconds(5)) //开滑动窗口
                .aggregate(new ItemCountAgg(), new WindowItemCountResult());//聚合函数

        //收集统一窗口所有商品count数据，并排序 TOP N
        DataStream<String> resultStream = windowAggStream
                .keyBy("windowEnd")
                .process(new TopNHotItems(5));

        resultStream.print();
        env.execute("HotItem");

    }

    //自定义增量聚合函数
    public static class ItemCountAgg implements AggregateFunction<UserBehavior, Long, Long>{
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior userBehavior, Long aLong) {
            return aLong + 1;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return aLong + acc1;
        }
    }

    //自定义全窗口函数
    public static class WindowItemCountResult implements WindowFunction<Long,ItemViewCount, Tuple, TimeWindow>{
        @Override
        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Long> iterable, Collector<ItemViewCount> collector) throws Exception {
            Long itemId = tuple.getField(0);
            Long windEnd = timeWindow.getEnd();
            Long count = iterable.iterator().next();
            collector.collect(new ItemViewCount(itemId, windEnd, count));
        }
    }

    public static class TopNHotItems extends KeyedProcessFunction<Tuple, ItemViewCount, String>{
        //定义top大小
        private Integer topSize;

        public TopNHotItems(Integer topSize) {
            this.topSize = topSize;
        }

        //定义列表状态 保存当前窗口内所有输出的ItemViewCount
        ListState<ItemViewCount> itemViewCountListState;

        @Override
        public void open(Configuration parameters) throws Exception {
            itemViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<ItemViewCount>("aa-cc",ItemViewCount.class));
        }

        @Override
        public void processElement(ItemViewCount itemViewCount, Context context, Collector<String> collector) throws Exception {
            //加入list
            itemViewCountListState.add(itemViewCount);
            context.timerService().registerEventTimeTimer(itemViewCount.getWindEnd() + 1);
        }

        @Override

        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //拷贝list
            ArrayList<ItemViewCount> itemViewCounts = Lists.newArrayList(itemViewCountListState.get().iterator());

            itemViewCounts.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount itemViewCount, ItemViewCount t1) {
                    return t1.getCount().intValue() - itemViewCount.getCount().intValue();
                }
            });

            //格式化输出
            StringBuilder str = new StringBuilder();
            str.append("==================");
            str.append("窗口结束时间：").append(new TimeStamp(timestamp - 1)).append("\n");
            //遍历前五
            for(int i = 0; i < Math.min(5, itemViewCounts.size()); i++){
                ItemViewCount itemViewCount = itemViewCounts.get(i);
                str.append("NO：").append(i+1).append(":");
                str.append("商品id：").append(itemViewCount.getItemId());
                str.append("热门度").append(itemViewCount.getCount()).append("\n");
            }
        }
    }


}
