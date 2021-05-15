package com.sensorsdata.flink.SQL;

/**
 * @Author: Li Guangwei
 * @Descriptions: TODO
 * @Date: 2021/5/11 10:45
 * @Version: 1.0
 */
public class MaxMin {
    private Integer max;
    private Integer min;

    public MaxMin() {
    }

    public MaxMin(Integer max, Integer min) {
        this.max = max;
        this.min = min;
    }

    public Integer getMax() {
        return max;
    }

    public void setMax(Integer max) {
        this.max = max;
    }

    public Integer getMin() {
        return min;
    }

    public void setMin(Integer min) {
        this.min = min;
    }

    @Override
    public String toString() {
        return "MaxMin{" +
                "max=" + max +
                ", min=" + min +
                '}';
    }
}
