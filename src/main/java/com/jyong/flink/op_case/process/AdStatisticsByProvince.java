package com.jyong.flink.op_case.process;

import com.jyong.flink.op_case.beans.AdClickEvent;
import com.jyong.flink.op_case.beans.AdCountViewByProvince;
import com.jyong.flink.op_case.beans.BlackListUserWarning;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @Author jyong
 * @Date 2023/6/22 21:20
 * @desc 页面广告分析-页面广告点击量统计
 */

public class AdStatisticsByProvince {

    public static void main(String[] args) throws Exception {


        //1.坏境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<AdClickEvent> dataSource = env.readTextFile("/Users/jyong/Desktop/jyong/workplace/coding/Jyong/UserBehaviorAnalysis/HotItemsAnalysis/src/main/resources/data/AdClickLog.csv")
                .map(new MapFunction<String, AdClickEvent>() {
                    @Override
                    public AdClickEvent map(String s) throws Exception {
                        /**
                         * 937166,1715,beijing,beijing,1511661602
                         */
                        String[] splits = s.split(",");
                        String userId = splits[0];
                        String adId = splits[1];
                        String province = splits[2];
                        String city = splits[3];
                        Long timestamp = Long.valueOf(splits[4]);
                        return new AdClickEvent(userId, adId, province, city, timestamp);
                    }
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<AdClickEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner(new SerializableTimestampAssigner<AdClickEvent>() {
                            @Override
                            public long extractTimestamp(AdClickEvent adClickEvent, long l) {
                                return adClickEvent.getTimestamp() * 1000;
                            }
                        })

                );
        //2.对同一个用户点击同一个广告的行为进行检测报警
        SingleOutputStreamOperator<AdClickEvent> filterAdClickStream = dataSource.keyBy(new KeySelector<AdClickEvent, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> getKey(AdClickEvent adClickEvent) throws Exception {
                return Tuple2.of(adClickEvent.getUserId(), adClickEvent.getAdId());
            }
        }).process(new FilterBlackListUser(10));


        //获取黑名单数据
        filterAdClickStream.getSideOutput(new OutputTag<BlackListUserWarning>("blacklist") {
        } ).print("black-list-user:");

        //3.分组聚合统计，统计每小时，10分钟的点击量
        SingleOutputStreamOperator<AdCountViewByProvince> aggregate = filterAdClickStream.keyBy(AdClickEvent::getProvince)
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.seconds(10)))
                .aggregate(new AdStatisticsByProvinceAgg(), new AdStatisticsByProvinceAggResult());

        aggregate.print();


        env.execute();
    }

    private static class FilterBlackListUser extends KeyedProcessFunction<Tuple2<String, String>, AdClickEvent, AdClickEvent> {

        //定义属性：点击次数上线
        private Integer countUpperBound;

        public FilterBlackListUser(Integer countUpperBound) {
            this.countUpperBound = countUpperBound;
        }


        //定义状态，保存当前用户对某一广告的点击次数
        ValueState<Long> countState;
        //定义一个标志状态，保存当前用户是否已经被发送到了黑名单里
        ValueState<Boolean> isSendState;

        @Override
        public void open(Configuration parameters) throws Exception {

            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("countState", Long.class));
            isSendState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("isSendState", Boolean.class));
        }

        @Override
        public void processElement(AdClickEvent value, KeyedProcessFunction<Tuple2<String, String>, AdClickEvent, AdClickEvent>.Context ctx, Collector<AdClickEvent> out) throws Exception {

            //判断当前用户对同一广告的点击次数，如果不够上线，就count加1正常输出，如果达到上限，直接过滤掉，并侧输出流输出到黑名单
            //首先获取当前的count值
            Long curCount = countState.value() == null ? 0L : countState.value();
            boolean isSend = isSendState.value() != null && isSendState.value();


            //1.判断是否是第一个数据，如果是的话，注册一个第二条0点的定时器，用于清除之前的累计数据。

            if (curCount == 0) {
                Long ts = (ctx.timerService().currentProcessingTime() / (24 * 60 * 60 * 1000) + 1) * (24 * 60 * 60 * 1000) - 8 * 60 * 60 * 1000;
                ctx.timerService().registerEventTimeTimer(ts);
            }


            //2.判断是否报警
            if (curCount >= countUpperBound) {
                //判断是否输出到被名单过，如果没有就输出到侧输出流
                if (!isSend) {
                    isSendState.update(true);
                    ctx.output(new OutputTag<BlackListUserWarning>("blacklist") {
                    }, new BlackListUserWarning(value.getUserId(), value.getAdId(), "click over " + countUpperBound + " times"));
                }
                //过滤掉数据，不再执行下面操作
                return;
            }

            //如果没有返回，点击次数加1，更新状态，正常输出数据到主流
            countState.update(curCount + 1);
            out.collect(value);

        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Tuple2<String, String>, AdClickEvent, AdClickEvent>.OnTimerContext ctx, Collector<AdClickEvent> out) throws Exception {

            //清空状态
            countState.clear();
            isSendState.clear();

        }
    }

    //自定义实现ProcessWindowFunction
    private static class AdStatisticsByProvinceAggResult extends ProcessWindowFunction<Long, AdCountViewByProvince, String, TimeWindow> {
        @Override
        public void process(String province, ProcessWindowFunction<Long, AdCountViewByProvince, String, TimeWindow>.Context context, Iterable<Long> elements, Collector<AdCountViewByProvince> out) throws Exception {

            String windowEnd = String.valueOf(context.window().getEnd());
            Long count = elements.iterator().next();
            out.collect(new AdCountViewByProvince(province, windowEnd, count));
        }
    }

    //自定义实现AggregateFunction
    private static class AdStatisticsByProvinceAgg implements AggregateFunction<AdClickEvent, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(AdClickEvent adClickEvent, Long accumulator) {
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
