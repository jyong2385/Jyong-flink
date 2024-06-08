package com.jyong.flink.op_case.process;

import com.jyong.flink.op_case.beans.UserBehavior;
import com.jyong.flink.op_case.beans.UserViewCount;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

/**
 * @Author jyong
 * @Date 2023/6/11 12:19
 * @desc 网站用户访问量
 */

public class UserViewCountAnalysis {

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

        /**4.分组开窗聚合，得到每个窗口内各个商品的count值
         * 利用key by优化处理
         */
        SingleOutputStreamOperator<UserViewCount> outputStreamOperator = dataStream
                //过滤掉不必要的数据,过滤掉pv行为
                .filter(new FilterFunction<UserBehavior>() {
                    @Override
                    public boolean filter(UserBehavior userBehavior) throws Exception {

                        return "uv".equals(userBehavior.getBehaivor());
                    }
                })
                .windowAll(TumblingEventTimeWindows.of(Time.hours(1)))
                .apply(new AllWindowFunction<UserBehavior, UserViewCount, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<UserBehavior> values, Collector<UserViewCount> out) throws Exception {

                        //定义set类型统计所有数据
                        Set<Long> set = new HashSet<>();
                        for (UserBehavior value : values) {
                            set.add(value.getUserId());
                        }
                        out.collect(new UserViewCount("uv", window.getEnd(), (long) set.size()));

                    }
                });

        //5.打印输出
        outputStreamOperator.print();

        env.execute();

    }

}
