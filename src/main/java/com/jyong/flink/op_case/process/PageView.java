package com.jyong.flink.op_case.process;

import com.jyong.flink.op_case.beans.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * @Author jyong
 * @Date 2023/6/11 11:00
 * @desc
 */

public class PageView {

    private static final String DATA_PATH = "/Users/jyong/Desktop/jyong/workplace/coding/Jyong/UserBehaviorAnalysis/HotItemsAnalysis/src/main/resources/data/UserBehavior.csv";

    public static void main(String[] args) throws Exception {

        //1.创建执行坏境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2.读取数据,创建DataStream数据流
        DataStreamSource<String> inputDataStream = env.readTextFile(DATA_PATH);

        //3.转换为POJO,分配事件戳和watermark
        SingleOutputStreamOperator<UserBehavior> dataStream = inputDataStream.map(data -> {
            String[] split = data.split(",");

            return new UserBehavior(Long.parseLong(split[0]), Long.parseLong(split[1]), Integer.parseInt(split[2]), split[3], Long.parseLong(split[4]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                    @Override
                    public long extractTimestamp(UserBehavior userBehavior, long l) {
                        return userBehavior.getTimestamp() * 1000;
                    }
                })
        );

        //4.分组开窗聚合，得到每个窗口内各个商品的count值
        SingleOutputStreamOperator<String> outputStreamOperator = dataStream
                .windowAll(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new AggFun(), new AggResult());


        //5.打印输出
        outputStreamOperator.print();

        env.execute();

    }

    private static class AggResult extends ProcessAllWindowFunction<Long,String, TimeWindow>{


        @Override
        public void process(ProcessAllWindowFunction<Long, String, TimeWindow>.Context context, Iterable<Long> elements, Collector<String> out) throws Exception {

            out.collect("开始时间："+new Timestamp(context.window().getStart())+" 结束时间："+new Timestamp(context.window().getEnd())+" 访问量:"+elements.iterator().next());

        }

    }

    private static class AggFun implements AggregateFunction<UserBehavior, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior userBehavior, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long accumulator1, Long accumulator2) {
            return accumulator1 + accumulator2;
        }
    }

}
