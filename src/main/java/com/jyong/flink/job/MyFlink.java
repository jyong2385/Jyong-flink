package com.jyong.flink.job;

import cn.hutool.core.util.StrUtil;
import com.jyong.flink.sink.MySink;
import com.jyong.flink.source.MySource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by jyong on 2021/1/10 19:58
 */
public class MyFlink {


    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStreamSource<String> streamSource = env.addSource(new MySource());

        SingleOutputStreamOperator<String> map = streamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {

                return StrUtil.concat(true, s.hashCode() + "", "----", s);
            }
        });



        map.addSink(new MySink());

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
