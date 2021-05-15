package com.sensorsdata.flink.bilibili.Async;

import java.util.Random;

/**
 * @description
 * @author: liguangwei
 * @create: 2021-03-03 17:15
 **/
public class Sleeps {
    public void test() throws InterruptedException {
        Thread.sleep(new Random().nextInt(100)*50);
    }
}
