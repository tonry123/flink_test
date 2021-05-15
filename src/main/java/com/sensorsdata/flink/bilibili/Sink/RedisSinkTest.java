package com.sensorsdata.flink.bilibili.Sink;

import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @description
 * @author: liguangwei
 * @create: 2021-03-07 11:33
 **/
public class RedisSinkTest implements RedisMapper<String> {

    //使用redis的数据结构
    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.SET);
    }

    //key
    @Override
    public String getKeyFromData(String s) {
        return s;
    }

    //value
    @Override
    public String getValueFromData(String s) {
        return "aaaaa";
    }
}
