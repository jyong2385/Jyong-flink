package com.jyong.flink.op_case.process;

import com.jyong.flink.op_case.beans.ApacheLogEvent;
import com.jyong.flink.op_case.beans.PageViewCount;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @Author jyong
 * @Date 2023/6/6 19:42
 * @desc 基于服务器日志的热门页面浏览量统计
 */

public class HotPageAnalysis {

    private static final String DATA_PATH = "/Users/jyong/Desktop/jyong/workplace/coding/Jyong/UserBehaviorAnalysis/HotItemsAnalysis/src/main/resources/data/apache.log";


    public static void main(String[] args) throws Exception {

        //1.创建流式执行坏境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.接入数据源、设置watermark
        SingleOutputStreamOperator<ApacheLogEvent> dataStream = env.readTextFile(DATA_PATH)
                .map(new MapFunction<String, ApacheLogEvent>() {
                    @Override
                    public ApacheLogEvent map(String s) throws Exception {
                        /**
                         * 83.149.9.216 - - 17/05/2015:10:05:00 +0000 GET /presentations/logstash-monitorama-2013/images/redis.png
                         */
                        String[] splits = s.split(" ");
                        String ip = splits[0];
                        String userId = splits[1];
                        String time = splits[3];
                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
                        long timestamp = simpleDateFormat.parse(time).getTime();
                        String method = splits[5];
                        String url = splits[6];
                        return new ApacheLogEvent(ip, userId, url, method, timestamp);
                    }
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<ApacheLogEvent>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                        .withTimestampAssigner(new SerializableTimestampAssigner<ApacheLogEvent>() {
                            @Override
                            public long extractTimestamp(ApacheLogEvent apacheLogEvent, long l) {
                                return apacheLogEvent.getTimestamp();
                            }
                        })
                );

        //对于迟到的数据发送到侧输出流中进行处理
        OutputTag<ApacheLogEvent> eventOutputTag = new OutputTag<ApacheLogEvent>("late") {
        };

        //2.分组开创聚合
        SingleOutputStreamOperator<PageViewCount> aggregateDataStream = dataStream.filter(ele -> "GET" .equalsIgnoreCase(ele.getMethod()))
                .keyBy(ApacheLogEvent::getUrl)
//                .timeWindow(Time.minutes(10), Time.seconds(10))
                .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.seconds(10)))
                .allowedLateness(Time.minutes(1))  //允许1分钟的延迟
                .sideOutputLateData(eventOutputTag)
                .aggregate(new PageCountAgg(), new PageCountResult());

        //处理迟到数据
        DataStream<ApacheLogEvent> sideOutputStream = aggregateDataStream.getSideOutput(eventOutputTag);
        sideOutputStream.print("迟到数据：");


        //取top n
        aggregateDataStream.keyBy(PageViewCount::getWindowEnd)
                .process(new TopNPages(3))
                .print();

        env.execute();

    }


    //自定义实现agg函数
    private static class PageCountAgg implements AggregateFunction<ApacheLogEvent, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ApacheLogEvent apacheLogEvent, Long accumulator) {
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

    //自定义实现windowfunction

    private static class PageCountResult implements WindowFunction<Long, PageViewCount, String, TimeWindow> {
        @Override
        public void apply(String url, TimeWindow timeWindow, Iterable<Long> iterable, Collector<PageViewCount> collector) throws Exception {
            collector.collect(new PageViewCount(url, timeWindow.getEnd(), iterable.iterator().next()));
        }
    }

    //自定义预聚合函数
    private static class TopNPages extends KeyedProcessFunction<Long, PageViewCount, String> {


        /**
         * 自定义top n的大小
         */
        private Integer topSize;

        public TopNPages(Integer topSize) {
            this.topSize = topSize;
        }

        //定义状态，保存数据到list，用于后续的top n
//        ListState<PageViewCount> pageViewCountListState;
        //优化处理，状态数据
        MapState<String, Long> pageViewCountMapState;

        @Override
        public void open(Configuration parameters) throws Exception {
//            pageViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<PageViewCount>("pageViewCountListState", PageViewCount.class));
            pageViewCountMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>("pageViewCountMapState", String.class, Long.class));

        }

        @Override
        public void processElement(PageViewCount pageViewCount, KeyedProcessFunction<Long, PageViewCount, String>.Context context, Collector<String> collector) throws Exception {
//            pageViewCountListState.add(pageViewCount);
            pageViewCountMapState.put(pageViewCount.getUrl(), pageViewCount.getCount());
            context.timerService().registerEventTimeTimer(pageViewCount.getWindowEnd() + 1);
            //定义一个定时器，1分钟后窗口关闭后，进行清除状态
            context.timerService().registerEventTimeTimer(pageViewCount.getWindowEnd() + 60 * 1000);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, PageViewCount, String>.OnTimerContext ctx, Collector<String> out) throws Exception {


            //判断是否到了窗口清理时间，如果时，直接清空状态返回
            if (timestamp == ctx.getCurrentKey() + 60 * 1000) {
                pageViewCountMapState.clear();
                out.collect("WARN:状态清空 ："+new Timestamp(timestamp));
                return;
            }


            ArrayList<Map.Entry<String, Long>> pageViewCounts = Lists.newArrayList(pageViewCountMapState.entries().iterator());
            pageViewCounts.sort(new Comparator<Map.Entry<String, Long>>() {
                @Override
                public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
                    if (o1.getValue() > o2.getValue())
                        return -1;
                    else if (o1.getValue() < o2.getValue())
                        return 1;
                    return 0;
                }
            });
//            pageViewCounts.sort(new Comparator<PageViewCount>() {
//                @Override
//                public int compare(PageViewCount o1, PageViewCount o2) {
//                    if (o1.getCount() > o2.getCount())
//                        return -1;
//                    else if (o1.getCount() < o2.getCount())
//                        return 1;
//                    return 0;
//                }
//            });

            //包装结果输出
            StringBuilder sb = new StringBuilder();
            sb.append("=============================\n");
            sb.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n");
            for (int i = 0; i < Math.min(topSize, pageViewCounts.size()); i++) {
                Map.Entry<String, Long> entry = pageViewCounts.get(i);
                sb.append("NO：").append(i + 1);
                sb.append(" 页面url：").append(entry.getKey());
                sb.append(" 浏览量：").append(entry.getValue());
                sb.append("\n");
            }
            sb.append("=============================\n");
            //控制输出频率
            TimeUnit.SECONDS.sleep(1);
            out.collect(sb.toString());


        }
    }


}
