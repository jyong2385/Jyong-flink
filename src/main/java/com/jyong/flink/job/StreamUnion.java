package com.jyong.flink.job;

import com.jyong.flink.entity.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * @Author jyong
 * @Date 2023/5/5 21:07
 * @desc 流的合并
 */

public class StreamUnion {

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
        SingleOutputStreamOperator<Event> stream2 = env.socketTextStream("ip", 8888).map(
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

        //5.进行合流
        DataStream<Event> eventDataStream = stream1.union(stream2);

        //6.逻辑处理
        eventDataStream.print("union stream -> ");

        //7.触发执行
        env.execute();


    }




}
