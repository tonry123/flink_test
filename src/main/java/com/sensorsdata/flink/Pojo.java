package com.sensorsdata.flink;

/**
 * @description
 * @author: liguangwei
 * @create: 2021-02-21 20:47
 **/
public class Pojo {
    public String t1;
    public Integer t2;

    public Pojo() {
    }

    public Pojo(String t1, Integer t2) {
        this.t1 = t1;
        this.t2 = t2;
    }

    public String getT1() {
        return t1;
    }

    public void setT1(String t1) {
        this.t1 = t1;
    }

    public Integer getT2() {
        return t2;
    }

    public void setT2(Integer t2) {
        this.t2 = t2;
    }

    @Override
    public String toString() {
        return "Pojo{" +
                "t1='" + t1 + '\'' +
                ", t2=" + t2 +
                '}';
    }
}
