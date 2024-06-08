package com.jyong.flink.job.operator;

import com.jyong.flink.entity.Event;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: jyong
 * @description flink分区策略
 * @date: 2023/3/26 12:14
 * flink8种分区策略
 * * globalpartitioner：分区器会将数据发送到下游的第一个taskid=0的算子实例
 * * shufflepartitioner:分区器会将数据随机发送到下游的某个算子
 * * reblancepartitioner：分区器将数据轮训发送到下游的所有算子
 * * brodcastpartitioner：分区器将数据发送到下游所有算子
 * rescle
 */
public class PhysicalPartition {
    public static void main(String[] args) throws Exception {

        //1.创建流式执行坏境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        //2.从元素中读取数据
        DataStreamSource<Event> eventDataStreamSource = env.fromElements(
                new Event("lisi", "/cat", 1000L),
                new Event("zhangsan", "/cat", 2000L),
                new Event("zhangsan", "/cat", 3000L),
                new Event("lisi", "/cat", 4000L),
                new Event("tianliu", "/cat", 5000L),
                new Event("tianliu", "/cat", 6000L),
                new Event("tianliu", "/cat", 7000L),
                new Event("tianliu", "/cat", 8000L),
                new Event("wangwu", "/index", 9000L)
        );


        eventDataStreamSource
                //1.shuffle分区策略：分区器将数据随机发送到下游的某个算子
//                .shuffle()

                //2.global分区策略：分区器将数据随机发送到下游的taskid=0的算子实例
//                .global()

                //3.reblance分区策略：分区器轮询将数据发送下游的每个分区
//                .rebalance()

                //4.broadcast分区策略：分区器将所有数据发送到下游的每个分区
//                .broadcast()

                //5.rescale:重缩放分区，分组+轮询
//                .rescale()

                //6.forward,发送到下游的第一个task，不允许改变下游的并行度，下游与上游任务并行度保持一致
//                .forward()

                //7.KeyGroupStream，根据key的分组发送
//                .keyBy()
                //8.partitionCustom 自定义分组
                .partitionCustom(new Partitioner<String>() {
                    @Override
                    public int partition(String key, int numPartition) {
                        //根据key的hashcode值进行分区
                        return key.hashCode() % numPartition > 0 ? key.hashCode() % numPartition : -(key.hashCode() % numPartition);
                    }
                }, new KeySelector<Event, String>() {
                    @Override
                    public String getKey(Event event) throws Exception {
                        return event.getUser();
                    }
                })

                .print()
                .setParallelism(4);

        env.execute();

    }

}
