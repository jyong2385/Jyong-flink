package com.jyong.flink.op_case.process;

import com.jyong.flink.op_case.beans.PageViewCount;
import com.jyong.flink.op_case.beans.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Random;

/**
 * @Author jyong
 * @Date 2023/6/11 11:43
 * @desc 网站总体访问量统计
 */

public class PageViewPro {
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
        SingleOutputStreamOperator<PageViewCount> outputStreamOperator = dataStream
                //过滤掉不必要的数据,过滤掉pv行为
                .filter(new FilterFunction<UserBehavior>() {
                    @Override
                    public boolean filter(UserBehavior userBehavior) throws Exception {

                        return "pv".equals(userBehavior.getBehaivor());
                    }
                })
                .map(new MapFunction<UserBehavior, Tuple2<Integer, Long>>() {
                    @Override
                    public Tuple2<Integer, Long> map(UserBehavior userBehavior) throws Exception {
                        //将数据打散，优化
                        Random random = new Random();
                        return Tuple2.of(random.nextInt(10), 1L);
                    }
                })
                .keyBy(data -> data.f0)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new AggFun(), new AggResult());


        //将聚合后的数据根据窗口结束时间聚合
        SingleOutputStreamOperator<PageViewCount> count = outputStreamOperator
                .keyBy(PageViewCount::getWindowEnd)
//                .sum("count");  //这样输出有先问题会有每个key的结果输出 需要处理成，所有结果都到齐后再全部输出
                .process(new TotalPvCount());

        //5.打印输出
        count.print();

        env.execute();

    }

    /**
     * 将相同窗口分组统计的count值进行叠加
     */
    private static class TotalPvCount extends KeyedProcessFunction<Long, PageViewCount, PageViewCount> {

        //定义状态，保存当前的总count值
        ValueState<Long> totalValueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            totalValueState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("totalValueState", Long.class));
        }

        @Override
        public void processElement(PageViewCount value, KeyedProcessFunction<Long, PageViewCount, PageViewCount>.Context ctx, Collector<PageViewCount> out) throws Exception {

            totalValueState.update(totalValueState.value() == null ? value.getCount() : totalValueState.value() + value.getCount());

            /**
             * 注册定时器：这里之需要注册，不需要担心重复注册，因为flink再同一个time时间内只会注册一个定时器
             */
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, PageViewCount, PageViewCount>.OnTimerContext ctx, Collector<PageViewCount> out) throws Exception {
            //定时器触发，所有分组count值都到齐了，直接输出即可

            //这个值就是最终的结果
            Long value = totalValueState.value();

            out.collect(new PageViewCount("total", ctx.getCurrentKey(), value));

            //处理完毕后，手动清空状态，处理完的无用数据及时清理，不占用资源
            totalValueState.clear();


        }
    }

    private static class AggResult implements WindowFunction<Long, PageViewCount, Integer, TimeWindow> {
        @Override
        public void apply(Integer integer, TimeWindow window, Iterable<Long> input, Collector<PageViewCount> out) throws Exception {
            out.collect(new PageViewCount(integer.toString(), window.getEnd(), input.iterator().next()));
        }

    }

    private static class AggFun implements AggregateFunction<Tuple2<Integer, Long>, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Tuple2<Integer, Long> integerLongTuple2, Long accumulator) {
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


/**
 * 开始时间：2017-11-26 09:00:00.0 结束时间：2017-11-26 10:00:00.0 访问量:41890
 * 开始时间：2017-11-26 10:00:00.0 结束时间：2017-11-26 11:00:00.0 访问量:48022
 * 开始时间：2017-11-26 11:00:00.0 结束时间：2017-11-26 12:00:00.0 访问量:47298
 * 开始时间：2017-11-26 12:00:00.0 结束时间：2017-11-26 13:00:00.0 访问量:44499
 * 开始时间：2017-11-26 13:00:00.0 结束时间：2017-11-26 14:00:00.0 访问量:48649
 * 开始时间：2017-11-26 14:00:00.0 结束时间：2017-11-26 15:00:00.0 访问量:50838
 * 开始时间：2017-11-26 15:00:00.0 结束时间：2017-11-26 16:00:00.0 访问量:52296
 * 开始时间：2017-11-26 16:00:00.0 结束时间：2017-11-26 17:00:00.0 访问量:52552
 * 开始时间：2017-11-26 17:00:00.0 结束时间：2017-11-26 18:00:00.0 访问量:48292
 * 开始时间：2017-11-26 18:00:00.0 结束时间：2017-11-26 19:00:00.0 访问量:13
 */