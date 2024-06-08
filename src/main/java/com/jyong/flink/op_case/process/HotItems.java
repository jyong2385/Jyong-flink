package com.jyong.flink.op_case.process;

import com.jyong.flink.op_case.beans.ItemViewCount;
import com.jyong.flink.op_case.beans.UserBehavior;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.*;

/**
 * @Author jyong
 * @Date 2023/5/31 20:53
 * @desc 热门商品统计
 *  统计每小时 5分钟内的商品热度排行 取top10
 */

public class HotItems {

    public static void main(String[] args) throws Exception {

        //1.创建执行坏境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2.读取数据,创建DataStream数据流
        DataStreamSource<String> inputDataStream = env.readTextFile("/Users/jyong/Desktop/jyong/workplace/coding/Jyong/UserBehaviorAnalysis/data/UserBehavior.csv");

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
        SingleOutputStreamOperator<ItemViewCount> aggregateStream = dataStream
                //过滤掉不必要的数据,过滤掉pv行为
                .filter(new FilterFunction<UserBehavior>() {
                    @Override
                    public boolean filter(UserBehavior userBehavior) throws Exception {
                        return "pv".equals(userBehavior.getBehaivor());
                    }
                })
                .keyBy(userBehavior -> userBehavior.getItemId())
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                .aggregate(new ItemCountAgg(), new WindowItemCountResult());

        //5.收集同一窗口的所有商品的count数据，排序输出top n
        SingleOutputStreamOperator<String> process = aggregateStream.keyBy(data -> data.getWindowEnd())
                .process(new TopNHotItems(10));

        process.print();


        env.execute("HotItems");
    }

    //自定义实现KeyedProcessFunction
    public static class TopNHotItems extends KeyedProcessFunction<Long, ItemViewCount, String> {

        //定义top n的大小
        private Integer topSize;

        public TopNHotItems(Integer topSize) {
            this.topSize = topSize;
        }

        //定义列表状态，保存当前窗口内所有输出到的ItemViewCount
        ListState<ItemViewCount> itemViewCountListState;

        @Override
        public void open(Configuration parameters) throws Exception {
            itemViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<ItemViewCount>("itemViewCountListState", ItemViewCount.class));

        }

        @Override
        public void processElement(ItemViewCount itemViewCount, KeyedProcessFunction<Long, ItemViewCount, String>.Context context, Collector<String> collector) throws Exception {

            //每来一条数据，存入list，并注册定时器
            itemViewCountListState.add(itemViewCount);
            context.timerService().registerEventTimeTimer(itemViewCount.getWindowEnd() + 1);

        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, ItemViewCount, String>.OnTimerContext ctx, Collector<String> out) throws Exception {

            List<ItemViewCount> itemViewCounts = Lists.newArrayList(itemViewCountListState.get().iterator());
            //将排名信息格式化打印
            StringBuilder sb = new StringBuilder();
            List<ItemViewCount> resultItemViewCounts = itemViewCounts.subList(0, topSize);
            sb.append("=============================\n");
            sb.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n");
            for (int i = 0; i < resultItemViewCounts.size(); i++) {
                ItemViewCount itemViewCount = resultItemViewCounts.get(i);
                sb.append("NO：").append(i+1);
                sb.append(" 商品ID："+itemViewCount.getItemId());
                sb.append(" 热门度："+itemViewCount.getCount());
                sb.append("\n");
            }
            sb.append("=============================\n");
            //控制输出频率
            Thread.sleep(1000L);

            out.collect(sb.toString());

        }
    }

    //自定义全窗口函数
    private static class WindowItemCountResult implements WindowFunction<Long, ItemViewCount, Long, TimeWindow> {

        @Override
        public void apply(Long itemId, TimeWindow timeWindow, Iterable<Long> iterable, Collector<ItemViewCount> collector) throws Exception {
            Long windowEnd = timeWindow.getEnd();
            Long count = iterable.iterator().next();
            collector.collect(new ItemViewCount(itemId, windowEnd, count));
        }
    }

    //实现自定义聚合函数
    private static class ItemCountAgg implements AggregateFunction<UserBehavior, Long, Long> {
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
