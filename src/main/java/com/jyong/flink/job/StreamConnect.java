package com.jyong.flink.job;

import com.jyong.flink.entity.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

/**
 * @Author jyong
 * @Date 2023/5/5 21:21
 * @desc
 */

public class StreamConnect {

    public static void main(String[] args) throws Exception {

        //1.创建执行坏境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.设置并行度
        env.setParallelism(1);

        //3.获取分流1
        SingleOutputStreamOperator<Event> stream1 = env.socketTextStream("ip", 9999).map(
                data -> {
                    String[] split = data.split(",");
                    return new Event(split[0].trim(), split[1].trim(), Long.valueOf(split[2].trim()));
                }
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event event, long l) {
                        return event.getTimestamp();
                    }
                })
        );

        //4.h获取分流2
        SingleOutputStreamOperator<List<String>> stream2 = env.socketTextStream("ip", 8888).map(
                data -> {
                    String[] split = data.split(",");
                    return Arrays.asList(split);
                }
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<List<String>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<List<String>>() {
                    @Override
                    public long extractTimestamp(List<String> events, long l) {
                        return Long.valueOf(events.get(2));
                    }
                })
        );

        //5.进行合流
        ConnectedStreams<Event, List<String>> connectedStreams = stream1.connect(stream2);

        //6.合流处理
        SingleOutputStreamOperator<Event> mapDataStream = connectedStreams.map(new CoMapFunction<Event, List<String>, Event>() {
            @Override
            public Event map1(Event event) throws Exception {

                return event;
            }

            @Override
            public Event map2(List<String> datas) throws Exception {
                return new Event(datas.get(0), datas.get(1), Long.valueOf(datas.get(0)));
            }
        });

        //7.流处理逻辑
        mapDataStream.print("stream->");

        //8.触发执行
        env.execute();


    }


}
