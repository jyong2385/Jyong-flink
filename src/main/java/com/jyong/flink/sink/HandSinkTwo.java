package com.jyong.flink.sink;

import com.jyong.flink.source.HandSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * @author ：intsmaze
 * @date ：Created in 2020/2/12 11:16
 * @description： https://www.cnblogs.com/intsmaze/
 * @modified By：
 */
public class HandSinkTwo implements SinkFunction<String> {

    public void invoke(String value, Context context) {
        System.out.println("sink---------"+value);
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);
        DataStreamSource<String> stringDataStreamSource = env.addSource(new HandSource());

        stringDataStreamSource.addSink(new HandSinkTwo());
        env.execute();
    }
}
