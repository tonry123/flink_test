package com.sensorsdata.flink.bilibili.Async;

import lombok.SneakyThrows;

import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

/**
 * @description
 * @author: liguangwei
 * @create: 2021-03-03 16:35
 **/
public class MyThread{

    public static void main(String[] args) {
        Run myThreadA = new Run("thread A");

        Run myThreadB = new Run("thread B");
        Run myThreadC = new Run("thread C");
        Run myThreadD = new Run("thread D");
//        Thread t1 = new Thread(myThreadA);
//        Thread t2 = new Thread(myThreadB);
//        Thread t3 = new Thread(myThreadC);
//        Thread t4 = new Thread(myThreadD);
//        t1.start();
//        t2.start();
//        t3.start();
//        t4.start();
        myThreadA.run();
        myThreadB.run();
        myThreadC.run();
        myThreadD.run();
    }
}

class  Run implements Runnable{
    private String name;

    public Run(String name) {

        this.name = name;
    }

    @SneakyThrows
    @Override
    public void run() {
        int i = 0;
        while (i < 5){
            Thread.sleep(new Random().nextInt(100)*50);
            i ++;
        }

    }
}

class test2{
    public Future<String> test2(String key) throws ExecutionException, InterruptedException {
        Callable ca2 = new Callable(){

            @Override
            public Object call() throws Exception {
                try {
                    Thread.sleep(new Random().nextInt(100)*50);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return key;
            }
        };
        FutureTask<String> ft2 = new FutureTask<String>(ca2);
        new Thread(ft2).start();

        return  ft2;
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        test2 test2 = new test2();
        System.out.println(test2.test2("4567890"));
    }


}


