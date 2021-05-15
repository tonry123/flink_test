package com.sensorsdata.flink.kafka.modle;

/**
 * @description
 * @author: liguangwei
 * @create: 2021-02-02 17:07
 **/
public class SingleMessage {

    private String name;
    private String message;
    private long timeLong;

    public long getTimeLong() {
        return timeLong;
    }

    public void setTimeLong(long timeLong) {
        this.timeLong = timeLong;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }


    @Override
    public String toString() {
        return "SingleMessage{" +
                "name='" + name + '\'' +
                ", message='" + message + '\'' +
                ", timeLong=" + timeLong +
                '}';
    }
}
