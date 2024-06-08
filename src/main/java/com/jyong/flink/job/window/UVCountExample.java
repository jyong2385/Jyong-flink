package com.jyong.flink.job.window;

import com.jyong.flink.entity.Event;
import com.jyong.flink.entity.UrlModel;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author: jyong
 * @description
 * @date: 2023/4/5 14:57
 */
public class UVCountExample {


    public static void main(String[] args) {

        //1.创建流式执行坏境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        env.setParallelism(1);

        env.getConfig().setAutoWatermarkInterval(100);

        //2.从元素中读取数据
        DataStream<Event> eventDataStreamSource = env.fromElements(new Event("zhangsan", "/index", 1000L),
                        new Event("lisi", "/cat", 1000L),
                        new Event("zhangsan", "/cat", 1000L),
                        new Event("zhangsan", "/cat", 2000L),
                        new Event("lisi", "/cat", 3000L),
                        new Event("tianliu", "/cat", 4000L),
                        new Event("wangwu", "/index", 1000L)
                )
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofMillis(10))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.getTimestamp();
                            }
                        })
                );


        eventDataStreamSource.print("input->");

        //统计每个url的访问量
        eventDataStreamSource.keyBy(Event::getUrl)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(10)))
                .aggregate(new UrlViewCountAgg(),new UrlViewCountResult());


    }

    /**
     * 增量聚合
     */
    private static class UrlViewCountAgg implements AggregateFunction<Event,Long,Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Event event, Long accumulator) {
            return accumulator+1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return null;
        }
    }

    /**
     * 包装窗口信息，输出UrlViewCount
     */
    private static class UrlViewCountResult extends ProcessWindowFunction<Long, UrlModel,String, TimeWindow> {
        @Override
        public void process(String url, ProcessWindowFunction<Long, UrlModel, String, TimeWindow>.Context context, Iterable<Long> iterable, Collector<UrlModel> collector) throws Exception {
            long start = context.window().getStart();
            long end = context.window().getEnd();
            Long uv = iterable.iterator().next();
            collector.collect(new UrlModel(url,uv,start,end));
        }
    }
}
