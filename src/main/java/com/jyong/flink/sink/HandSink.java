package com.jyong.flink.sink;

import com.jyong.flink.source.HandSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * @author ：intsmaze
 * @date ：Created in 2020/2/12 11:06
 * @description： https://www.cnblogs.com/intsmaze/
 * @modified By：
 */
public class HandSink extends RichSinkFunction<String> {


    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println(">>>>>>>>>>>>>>.初始化资源的连接");
    }


    @Override
    public void invoke(String value, Context context) {
        System.out.println("sink---------"+value);
    }


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);
        DataStreamSource<String> stringDataStreamSource = env.addSource(new HandSource());

        stringDataStreamSource.addSink(new HandSink());
        env.execute();

    }


}
