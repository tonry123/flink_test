package com.sensorsdata.flink.bilibili.Async;

import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * @description
 * @author: liguangwei
 * @create: 2021-03-03 16:34
 **/
public class AsyncIO {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> stringDataStreamSource = env.socketTextStream("10.120.138.222", 8888);

//        SingleOutputStreamOperator<String> ss = stringDataStreamSource.map(new MapFunction<String, String>() {
//            @Override
//            public String map(String s) throws Exception {
//                return s;
//            }
//        });

         SingleOutputStreamOperator<String> ss = AsyncDataStream.unorderedWait(stringDataStreamSource, new AsyncDatabaseRequest(), 10, TimeUnit.MICROSECONDS, 3);

        ss.print();

        env.execute("AsyncIO");

    }
}

// 这个例子使用 Java 8 的 Future 接口（与 Flink 的 Future 相同）实现了异步请求和回调。
/**
 * 实现 'AsyncFunction' 用于发送请求和设置回调。
 */
class AsyncDatabaseRequest extends RichAsyncFunction<String, String> {


    @Override
    public void asyncInvoke(String s, ResultFuture<String> resultFuture) throws Exception,ExecutionException, InterruptedException {
        // 发送异步请求，接收 future 结果
        test2 test2 = new test2();
        final Future<String> result = test2.test2(s);
        // 设置客户端完成请求后要执行的回调函数
        // 回调函数只是简单地把结果发给 future
        CompletableFuture.supplyAsync(new Supplier<String>() {
            @Override
            public String get() {
                try {
                    return result.get();
                } catch (InterruptedException | ExecutionException e) {
                    // 显示地处理异常。
                    return null;
                }
            }
        }).thenAccept( (String dbResult) -> {

            try {
                resultFuture.complete(Collections.singleton(result.get()));
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        });
    }
}


