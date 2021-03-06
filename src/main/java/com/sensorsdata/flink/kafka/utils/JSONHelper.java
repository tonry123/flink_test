package com.sensorsdata.flink.kafka.utils;

import com.alibaba.fastjson.JSONObject;
import com.sensorsdata.flink.kafka.modle.SingleMessage;



/**
 * @description
 * @author: liguangwei
 * @create: 2021-02-02 17:09
 **/
public class JSONHelper {

    /**
     * 解析消息，得到时间字段
     * @param raw
     * @return
     */
    public static long getTimeLongFromRawMessage(String raw){
        SingleMessage singleMessage = parse(raw);
        return null==singleMessage ? 0L : singleMessage.getTimeLong();
    }

    /**
     * 将消息解析成对象
     * @param raw
     * @return
     */
    public static SingleMessage parse(String raw){
        SingleMessage singleMessage = null;

        if (raw != null) {
            singleMessage = JSONObject.parseObject(raw, SingleMessage.class);
        }

        return singleMessage;
    }
}