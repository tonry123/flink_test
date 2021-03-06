package com.sensorsdata.flink.kafka.action;

import com.sensorsdata.flink.kafka.modle.SingleMessage;
import com.sensorsdata.flink.kafka.utils.Constants;
import com.sensorsdata.flink.kafka.utils.ConsumerProperties;
import com.sensorsdata.flink.kafka.utils.JSONHelper;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;


import javax.annotation.Nullable;
import java.util.Properties;


/**
 * @description
 * @author: liguangwei
 * @create: 2021-02-02 16:56
 **/
public class FlinkConsumerKakfa {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000); // ????????????????????????
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // ???????????????

// ???????????????exactly-once ?????????????????????
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

// ??????????????????????????????500 ms?????????
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

// ??????????????????????????????????????????????????????
        env.getCheckpointConfig().setCheckpointTimeout(60000);

// ??????????????????????????????????????????


        ConsumerProperties consumerProperties = new ConsumerProperties();
        Properties props = consumerProperties.getConsumerProperties();

//        DataStreamSource<String> source = env.socketTextStream("127.0.0.1",9999);

//        ???????????????????????????kafka??????????????????

//
//        DataStreamSource<String> consumer = env.addSource(new FlinkKafkaConsumer010<>(
//                Constants.KAFKA_TOPIC,
//                new SimpleStringSchema(),
//                props));

//        consumer.filter((value)->{
//            System.out.println(value+"=====----");
////            SingleMessage singleMessage;
////                try {
////                   singleMessage  = JSONHelper.parse(value);
////
////                }catch (Exception e){
////
////                    return false;
////                }
////                if(StringUtils.isNotBlank(singleMessage.getMessage()) && singleMessage.getMessage().equals("test6")){
////                    return true;
////                }
//            if(value.length() > 20){
//                return true;
//            }
//                return false;
//        }).map(value -> value.toUpperCase()).print();
//        env.execute("hahaha");
//        consumer.filter(new FilterFunction<String>() {
//            @Override
//            public boolean filter(String s) throws Exception {
//
//                System.out.println(s+"]]]]]]nn");
//                SingleMessage singleMessage = JSONHelper.parse(s);
//                System.out.println(singleMessage+"]]]]]]");
//                if(StringUtils.isNotBlank(singleMessage.getMessage()) && singleMessage.getMessage().equals("test6")){
//                    System.out.println(singleMessage);
//                    return true;
//                }
//                return false;
//            }
//        }).map(new MapFunction<String, Object>() {
//
//            @Override
//            public Object map(String s) throws Exception {
//                SingleMessage singleMessage = JSONHelper.parse(s);
//                System.out.println("====="+singleMessage);
//                return s;
//            }
//        }).print();

//        consumer.print();
//        env.execute("jajaja");
        FlinkKafkaConsumer011<String> consumer =
                new FlinkKafkaConsumer011<>(Constants.KAFKA_TOPIC, new SimpleStringSchema(), props);
////            ???????????????????????????
//        consumer.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<String> (){
//            @Override
//            public long extractTimestamp(String element, long previousElementTimestamp) {
//                return JSONHelper.getTimeLongFromRawMessage(element);
//            }
//
//            @Nullable
//            @Override
//            public Watermark checkAndGetNextWatermark(String lastElement, long extractedTimestamp) {
//                if (lastElement != null) {
//                    return new Watermark(JSONHelper.getTimeLongFromRawMessage(lastElement));
//                }
//                return null;
//            }
//        });


        env.addSource(consumer)
                //?????????????????????Tuple2??????????????????????????????????????????(???????????????????????????1)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {

                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                        System.out.println(s+"0000000000");
                        SingleMessage singleMessage;
                        try {
                            singleMessage = JSONHelper.parse(s);
                        }catch (Exception e){
                            e.getStackTrace();
                            return ;
                        }
                        System.out.println(singleMessage+"=====");
                        if (null != singleMessage) {
                            collector.collect(new Tuple2<String, Long>(singleMessage.getMessage(), 1L));
                        }
                    }
                })
            //???????????????key
                .keyBy(0)
              //  ???????????????2???
//                .timeWindow(Time.seconds(20))
//                .timeWindow(Time.seconds(20),Time.seconds(10))
                .countWindow(5,2)
               // ???????????????????????????????????????
//                .apply(new WindowFunction<Tuple2<String, Long>, Tuple2<String, Long>, Tuple, TimeWindow>() {
////
////                    @Override
////                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple2<String, Long>> iterable, Collector<Tuple2<String, Long>> collector) throws Exception {
////                        long sum = 0L;
////                        for (Tuple2<String, Long> record: iterable) {
////                            sum += record.f1;
////                        }
////
////                        Tuple2<String, Long> result = iterable.iterator().next();
////                        result.f1 = sum;
////                        collector.collect(result);
////                    }
////
////
////                })
                .sum(1)
                //???????????????STDOUT
                .print();



        env.execute("Flink-Kafka demo");

        }


}
