package com.sensorsdata.flink.shangguigu.processFunction.test.beans;

import scala.Int;

/**
 * @Author: Li Guangwei
 * @Descriptions: TODO
 * @Date: 2021/5/11 11:54
 * @Version: 1.0
 */
public class PV {
    private String name;
    private Long times;

    public PV() {
    }

    public PV(String name, Long times) {
        this.name = name;
        this.times = times;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getTimes() {
        return times;
    }

    public void setTimes(Long times) {
        this.times = times;
    }

    @Override
    public String toString() {
        return "PV{" +
                "name='" + name + '\'' +
                ", times=" + times +
                '}';
    }
}
