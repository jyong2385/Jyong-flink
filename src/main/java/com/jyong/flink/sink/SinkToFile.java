package com.jyong.flink.sink;

import com.jyong.flink.entity.Event;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

/**
 * @author: jyong
 * @description 输出到文件
 * @date: 2023/3/26 19:49
 */
public class SinkToFile {

    public static void main(String[] args) throws Exception {

        //1.创建流式执行坏境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);
        //2.从元素中读取数据
        DataStreamSource<Event> eventDataStreamSource = env.fromElements(new Event("zhangsan", "/index", 1000L),
                new Event("lisi", "/cat", 1000L),
                new Event("zhangsan", "/cat", 1000L),
                new Event("zhangsan", "/cat", 2000L),
                new Event("lisi", "/cat", 3000L),
                new Event("tianliu", "/cat", 4000L),
                new Event("wangwu", "/index", 1000L)
        );


        //3.创建一个filesink
        StreamingFileSink<String> fileSink = StreamingFileSink.<String>forRowFormat(new Path("./output/"), new SimpleStringEncoder<>("utf-8"))
                //指定滚动策略
                .withRollingPolicy(
                        //使用flink提供的策略
                        DefaultRollingPolicy.create()
                                //按照滚动大小
                                .withMaxPartSize(1024^3)
                                //不活跃多长时间后，滚动一次
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                                //间隔多长时间滚动一次
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(10))
                                .build()
                )
                .build();

        //4.注入sink
        eventDataStreamSource.map(Event::toString).addSink(fileSink);

        //5.触发执行
        env.execute();

    }
}
