package com.sensorsdata.flink.bilibili.test1.bean;

/**
 * @description
 * @author: liguangwei
 * @create: 2021-02-28 17:40
 **/
public class Order {

    private String id;
    private String name;
    private int age;


    public Order() {
    }

    public Order(String id, String name, int age) {
        this.id = id;
        this.name = name;
        this.age = age;
    }

    public static Order of(String id, String name, int age){
        return new Order(id,name,age);
    }

    @Override
    public String toString() {
        return "Order{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", age=" + age +
                '}';
    }
}
