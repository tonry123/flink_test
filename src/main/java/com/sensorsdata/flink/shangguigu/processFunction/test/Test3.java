package com.sensorsdata.flink.shangguigu.processFunction.test;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class Test3 {
    public static void main (String[] args) throws Exception {
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        if (parameterTool.getNumberOfParameters() < 5) {
            System.out.println("Missing parameters!\n" +
                    "Usage: Kafka --input-topic-data <topic> --input-topic-config <topic> --output-topic <topic> " +
                    "--bootstrap.servers <kafka brokers> " +
                    "--group.id <some id> --auto.offset.reset <latest, earliest, none>");
            return;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(parameterTool.getInt("checkpoint.interval",60000)); // create a checkpoint every n mill seconds

        // set mode to exactly-once (this is the default)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // make sure 500 ms of progress happen between checkpoints
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

        // checkpoints have to complete within one minute, or are discarded
        env.getCheckpointConfig().setCheckpointTimeout(60000);

        // allow only one checkpoint to be in progress at the same time
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(parameterTool);

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                parameterTool.getRequired("input-topic-data"),
                new SimpleStringSchema(),
                parameterTool.getProperties());
        FlinkKafkaConsumer<String> consumerBroadcast = new FlinkKafkaConsumer<>(
                parameterTool.getRequired("input-topic-config"),
                new SimpleStringSchema(),
                parameterTool.getProperties());

        DataStream<Tuple3<String, Integer, Long>> dataStream = env.addSource(consumer).flatMap(new LineSplitter());
        final MapStateDescriptor<String,Map<String,Object>> broadCastConfigDescriptor = new MapStateDescriptor<>("broadCastConfig",
                BasicTypeInfo.STRING_TYPE_INFO, new MapTypeInfo<>(String.class, Object.class));
        // e.g. {"length":5}
        BroadcastStream<Map<String,Object>> broadcastStream = env.addSource(consumerBroadcast).
                flatMap(new FlatMapFunction<String, Map<String, Object>>() {
                            // ?????? json ??????
                            private final ObjectMapper mapper = new ObjectMapper();

                            @Override
                            public void flatMap(String value, Collector<Map<String, Object>> out) {
                                try {
                                    out.collect(mapper.readValue(value, Map.class));
                                } catch (IOException e) {
                                    e.printStackTrace();
                                    System.out.println(value);
                                }
                            }
                        }
                ).broadcast(broadCastConfigDescriptor); //????????????????????????descriptor

        dataStream.keyBy(0).connect(broadcastStream).process(new KeyedBroadcastProcessFunction<String, Tuple3<String, Integer, Long>, Map<String, Object>, Tuple2<String,Integer>>() {
            private final Logger logger = LoggerFactory.getLogger(BroadCastWordCountExample.class);
            private transient MapState<String, Integer> counterState;
            int length = 5;
            // ?????????????????? broadCastConfigDescriptor ?????????????????? java.lang.IllegalArgumentException: The requested state does not exist ?????????
            private final MapStateDescriptor<String, Map<String,Object>> broadCastConfigDescriptor = new MapStateDescriptor<>("broadCastConfig", BasicTypeInfo.STRING_TYPE_INFO, new MapTypeInfo<>(String.class, Object.class));
            private final MapStateDescriptor<String, Integer> descriptor = new MapStateDescriptor<>("counter",String.class, Integer.class);
            @Override
            public void open(Configuration parameters) throws Exception{
                counterState = getRuntimeContext().getMapState(descriptor);
                logger.info("get counter/globalConfig MapState from checkpoint");
            }
            /**
             * ??????????????????????????????
             * */
            @Override
            public void processElement(Tuple3<String, Integer, Long> value, ReadOnlyContext ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                /**
                 * ???????????????????????? ReadOnlyBroadcastState????????? Flink ???????????????????????? BroadcastState ?????????
                 * */
                // ??????????????????????????????
                ReadOnlyBroadcastState<String, Map<String,Object>> broadcastState = ctx.getBroadcastState(broadCastConfigDescriptor);
                if (broadcastState.contains("broadcastStateKey")) {
                    length = (Integer) broadcastState.get("broadcastStateKey").get("length");
                }
                if (value.f0.length() > length) {
                    logger.warn("length of str {} > {}, ignored", value.f0, length);
                    return;
                }
                if (counterState.contains(value.f0)) {
                    counterState.put(value.f0, counterState.get(value.f0) + value.f1);
                } else {
                    counterState.put(value.f0, value.f1);
                }
                out.collect(new Tuple2<>(value.f0, counterState.get(value.f0)));
            }
            /**
             * ??????????????????????????????
             * */
            @Override
            public void processBroadcastElement(Map<String, Object> value, Context ctx, Collector<Tuple2<String,Integer>> out) throws Exception {
                if (!value.containsKey("length")) {
                    logger.error("stream element {} do not contents \"length\"", value);
                    return;
                }
 
                /*ctx.applyToKeyedState(broadCastConfigDescriptor, (key, state) -> {
                     // ???????????????????????? broadCastConfigDescriptor ????????? state
                });*/
                /** ???????????? BroadcastState???BroadcastState ?????? Map ???????????????????????????????????????????????????
                 * */
                BroadcastState<String, Map<String,Object>> broadcastState = ctx.getBroadcastState(broadCastConfigDescriptor);
                // ???????????????BroadcastState ????????? MapState.????????? broadcastStateKey ?????????????????? key, ????????????
                // ???????????????????????????????????????: BroadcastState
                if (broadcastState.contains("broadcastStateKey")) {
                    Map<String, Object> oldMap = broadcastState.get("broadcastStateKey");
                    logger.info("get State {}, replaced with State {}",oldMap,value);
                } else {
                    logger.info("do not find old State, put first counterState {}",value);
                }
                broadcastState.put("broadcastStateKey",value);
            }
        }).print();

        env.execute("BroadCastWordCountExample");
    }
}
