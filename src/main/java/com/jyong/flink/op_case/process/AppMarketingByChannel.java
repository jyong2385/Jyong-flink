package com.jyong.flink.op_case.process;

import com.jyong.flink.op_case.beans.ChannelPromotionCount;
import com.jyong.flink.op_case.beans.MarketingUserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @Author jyong
 * @Date 2023/6/14 21:38
 * @desc app市场推广统计 分渠道统计
 */

public class AppMarketingByChannel {

    public static void main(String[] args) throws Exception {


        //1.坏境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从自定义数据源中读取数据
        SingleOutputStreamOperator<MarketingUserBehavior> dataSource = env.addSource(new SimulatedMarketingUserBehaviorSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<MarketingUserBehavior>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<MarketingUserBehavior>() {
                            @Override
                            public long extractTimestamp(MarketingUserBehavior marketingUserBehavior, long l) {
                                return marketingUserBehavior.getTimestamp();
                            }
                        }));

        //3 分渠道开窗统计
        dataSource.keyBy(new KeySelector<MarketingUserBehavior, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(MarketingUserBehavior marketingUserBehavior) throws Exception {
                        return Tuple2.of(marketingUserBehavior.getChannel(), marketingUserBehavior.getBehavior());
                    }
                })
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.seconds(5)))
                .aggregate(new MarketCountAgg(), new MarketCOuntResult()).print();

        env.execute();
    }


    //实现自定义的全窗口函数
    private static class MarketCOuntResult extends ProcessWindowFunction<Long, ChannelPromotionCount, Tuple2<String, String>, TimeWindow> {


        @Override
        public void process(Tuple2<String, String> key, ProcessWindowFunction<Long, ChannelPromotionCount, Tuple2<String, String>, TimeWindow>.Context context, Iterable<Long> elements, Collector<ChannelPromotionCount> out) throws Exception {


            String channel = key.f0;
            String behavior = key.f1;
            long windowEnd = context.window().getEnd();
            String windowend = new Timestamp(windowEnd).toString();
            Long count = elements.iterator().next();
            out.collect(new ChannelPromotionCount(channel,behavior,windowend,count));
        }
    }


    //自定义实现聚合函数
    private static class MarketCountAgg implements AggregateFunction<MarketingUserBehavior, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(MarketingUserBehavior marketingUserBehavior, Long accumulator) {
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


    //实现自定义的模拟市场用户行为数据源
    private static class SimulatedMarketingUserBehaviorSource implements SourceFunction<MarketingUserBehavior> {

        //控制是否正常运行的标志位
        private Boolean running = true;

        //定义用户行为和渠道的范围
        List<String> behaviorList = Arrays.asList("CLICK", "DOWNLOAD", "INSTALL", "UNINSTALL");
        List<String> channelList = Arrays.asList("app store", "wechat", "weibo");

        Random random = new Random();

        @Override
        public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {


            while (true) {
                //随机生成所有字段
                long id = random.nextLong();
                String behavior = behaviorList.get(random.nextInt(behaviorList.size()));
                String channel = channelList.get(random.nextInt(channelList.size()));
                long timestamp = System.currentTimeMillis();

                //发出数据
                ctx.collect(new MarketingUserBehavior(id, behavior, channel, timestamp));

                TimeUnit.SECONDS.sleep(1);
            }


        }

        @Override
        public void cancel() {
            running = false;
        }
    }

}
