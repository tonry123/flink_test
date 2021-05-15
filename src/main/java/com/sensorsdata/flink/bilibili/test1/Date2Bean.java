package com.sensorsdata.flink.bilibili.test1;

import com.sensorsdata.flink.bilibili.test1.bean.Order;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.sql.*;

/**
 * @description
 * @author: liguangwei
 * @create: 2021-02-28 17:56
 **/
public class Date2Bean extends RichMapFunction<String, Order> {


    private transient Connection connection = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        String JDBC_DRIVER = "com.mysql.jdbc.Driver";

        //数据库URL：这里的tt是数据库名称
        String JDBC_URL = "jdbc:mysql://47.101.147.173:3306/lgw?useSSL=false&serverTimezone=UTC";

//        数据库的用户名与密码
        String USER = "root";
        String PASS = "Lgw912037325.";

        Class.forName(JDBC_DRIVER);

//            数据库的连接：通过DriverManager类的getConnection方法，传入三个参数：数据库URL、用户名、用户密码，实例化connection对象
        connection = DriverManager.getConnection(JDBC_URL,USER,PASS);

//            实例化statement对象
        System.out.println("======");

    }

    @Override
    public Order map(String id) throws Exception {
        PreparedStatement statement = (PreparedStatement) connection.prepareStatement("select name,age from user11 where id = ?");
        statement.setString(1,id);
        ResultSet resultSet = statement.executeQuery();
        String name = null;
        Integer age = -1;
        while (resultSet.next()){
            name = resultSet.getString("name");
            age = resultSet.getInt("age");
        }

        statement.close();


        return Order.of(id,name,age);
    }

    @Override
    public void close() throws Exception {
        super.close();
        connection.close();
    }
}
