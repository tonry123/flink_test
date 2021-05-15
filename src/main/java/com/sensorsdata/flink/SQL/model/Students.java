package com.sensorsdata.flink.SQL.model;

import lombok.Data;
import scala.Int;

/**
 * @description
 * @author: liguangwei
 * @create: 2021-02-03 16:02
 **/

public class Students {
    private String name;
    private Integer age;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    //必须要有个构造函数函数，不然会报错
    public Students() {
    }

    public Students(String name, Integer age) {
        this.name = name;
        this.age = age;
    }


    @Override
    public String toString() {
        return "Students{" +
                "name='" + name + '\'' +
                ", age=" + age +
                '}';
    }
}
