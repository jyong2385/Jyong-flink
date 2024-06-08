package com.jyong.flink.job;

import com.jyong.flink.entity.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author jyong
 * @Date 2023/5/8 20:51
 * @desc
 */

public class IntervalJoinExample {


    public static void main(String[] args) throws Exception{


        //1.创建执行坏境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.设置并行度
        env.setParallelism(1);

        //3.获取流
        SingleOutputStreamOperator<Tuple2<String, Long>> orderStream = env.fromElements(
                Tuple2.of("lisi", 1000L),
                Tuple2.of("zhangsan", 2000L),
                Tuple2.of("wangwu", 1000L),
                Tuple2.of("sunba", 1000L),
                Tuple2.of("lisi", 3000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple2<String, Long> tuple2, long l) {
                        return tuple2.f1;
                    }
                }));


        DataStream<Event> clickStream = env.fromElements(new Event("zhangsan", "/index", 1000L),
                new Event("lisi", "/cat", 1000L),
                new Event("zhangsan", "/cat", 1000L),
                new Event("zhangsan", "/cat", 2000L),
                new Event("lisi", "/cat", 3000L),
                new Event("tianliu", "/cat", 4000L),
                new Event("wangwu", "/index", 1000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.getTimestamp();
                    }
                })
        );

        //4. 合流操作
        SingleOutputStreamOperator<String> process = orderStream.keyBy(data -> data.f0)
                .intervalJoin(clickStream.keyBy(ele -> ele.getUser()))
                .between(Time.seconds(-5), Time.seconds(10))  //前5秒，后10秒 范围
                .process(new ProcessJoinFunction<Tuple2<String, Long>, Event, String>() {
                    @Override
                    public void processElement(Tuple2<String, Long> left, Event right, ProcessJoinFunction<Tuple2<String, Long>, Event, String>.Context ctx, Collector<String> out) throws Exception {


                        out.collect(right + " => " + left);
                    }
                });

        process.print();

        env.execute();


    }

}

/**
 * 输出结果：
 *
 * Event{user='zhangsan', url='/index', timestamp=1970-01-01 08:00:01.0} => (zhangsan,2000)
 * Event{user='lisi', url='/cat', timestamp=1970-01-01 08:00:01.0} => (lisi,1000)
 * Event{user='zhangsan', url='/cat', timestamp=1970-01-01 08:00:01.0} => (zhangsan,2000)
 * Event{user='zhangsan', url='/cat', timestamp=1970-01-01 08:00:02.0} => (zhangsan,2000)
 * Event{user='lisi', url='/cat', timestamp=1970-01-01 08:00:01.0} => (lisi,3000)
 * Event{user='lisi', url='/cat', timestamp=1970-01-01 08:00:03.0} => (lisi,1000)
 * Event{user='lisi', url='/cat', timestamp=1970-01-01 08:00:03.0} => (lisi,3000)
 * Event{user='wangwu', url='/index', timestamp=1970-01-01 08:00:01.0} => (wangwu,1000)
 *
 *
 *
 */
