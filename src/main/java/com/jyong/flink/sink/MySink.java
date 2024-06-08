package com.jyong.flink.sink;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * Created by jyong on 2021/1/10 19:59
 */
public class MySink implements SinkFunction {

    @Override
    public void invoke(Object value, Context context) {
        String s = value.toString();
        System.out.println("mySink->" + s);

    }
}
