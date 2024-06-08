package com.jyong.flink.job;

import com.jyong.flink.entity.Event;
import com.jyong.flink.source.ClickSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @Author jyong
 * @Date 2023/5/4 21:11
 * @desc 侧输出流案例
 * desc:按照不同的名字，提取出不同的流
 */

public class SideOutputStream {


    public static void main(String[] args) throws Exception {

        //1。创建坏境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2。设置并行度
        env.setParallelism(1);

        //3。设置watermark
        SingleOutputStreamOperator<Event> streamOperator = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.getTimestamp();
                            }
                        })
                );


        //4。定义侧输出流的标签
        OutputTag<Event> zTag = new OutputTag<Event>("zhangsan"){};
        OutputTag<Event> bTag = new OutputTag<Event>("lisi"){};

        //5。将特殊数据提取到到数据流中
        SingleOutputStreamOperator<Event> process = streamOperator.process(new ProcessFunction<Event, Event>() {
            @Override
            public void processElement(Event event, ProcessFunction<Event, Event>.Context context, Collector<Event> collector) throws Exception {

                String user = event.getUser();
                if (StringUtils.equals(user, "张三")) {
                    context.output(zTag, event);
                } else if (StringUtils.equals(user, "李四")) {
                    context.output(bTag, event);
                } else {
                    collector.collect(event);
                }

            }
        });

        //6。处理逻辑
        //主流
        process.print("main->");
        //侧输出流 zTag bTag
        DataStream<Event> zStream = process.getSideOutput(zTag);
        zStream.print("z->");

        DataStream<Event> bStream = process.getSideOutput(bTag);
        bStream.print("b->");

        //7。触发流
        env.execute();

    }
}
