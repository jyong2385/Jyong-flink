package com.jyong.flink.job;

import com.jyong.flink.entity.Event;
import com.jyong.flink.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author jyong
 * @Date 2023/5/17 21:03
 * @desc
 */

public class BufferingSinkExample {


    public static void main(String[] args) throws Exception {

        //1.创建执行坏境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.设置并行度
        env.setParallelism(1);


        //3.引入数据源
        DataStream<Event> eventDataStreamSource = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.getTimestamp();
                                    }
                                }));

        eventDataStreamSource.print("input");

        //4.批量缓存输出

        eventDataStreamSource.addSink(new BufferingSink(10L));

        //触发执行
        env.execute();

    }

    public static class BufferingSink implements SinkFunction<Event>, CheckpointedFunction {

        //设置一个阈值，达到多少条的时候输出
        public final Long size;

        public BufferingSink(Long size) {
            this.size = size;
            this.bufferedElements = new ArrayList<>();
        }

        /**
         * 缓存数据
         */
        private List<Event> bufferedElements;

        //定义一个算子状态
        private ListState<Event> checkpointedState;


        @Override
        public void invoke(Event value, Context context) throws Exception {

            //每来一条数据，缓存到列表
            bufferedElements.add(value);
            //判断如果达到阈值，就批量写入
            if (bufferedElements.size() == size) {
                //批量写入，利用打印模拟
                for (Event element : bufferedElements) {
                    System.out.println(element);
                }
                System.out.println("=========输出完毕========");
                bufferedElements.clear();
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {

            //提前清空状态
            checkpointedState.clear();
            ;

            //对缓存中的列表进行持久化,复制缓存列表到列表状态
            for (Event ele : bufferedElements) {
                checkpointedState.add(ele);
            }


        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            //定义算子状态
            checkpointedState = context.getOperatorStateStore().getListState(new ListStateDescriptor<Event>("list-state", Event.class));

            //如果是故障恢复，则将状态列表中的数据恢复到缓存列表中
            if (context.isRestored()) {
                for (Event event : checkpointedState.get()) {
                    bufferedElements.add(event);
                }
            }
        }
    }


}
