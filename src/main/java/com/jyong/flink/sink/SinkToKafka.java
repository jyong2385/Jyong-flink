package com.jyong.flink.sink;

import com.jyong.flink.entity.Event;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * @author: jyong
 * @description 输出到kafka
 * @date: 2023/3/26 20:04
 */
public class SinkToKafka {

    public static void main(String[] args) throws Exception {


        //1.创建流式执行坏境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);
        //2.从元素中读取数据
        DataStreamSource<Event> eventDataStreamSource = env.fromElements(new Event("zhangsan", "/index", 1000L),
                new Event("lisi", "/cat", 1000L),
                new Event("zhangsan", "/cat", 1000L),
                new Event("zhangsan", "/cat", 2000L),
                new Event("lisi", "/cat", 3000L),
                new Event("tianliu", "/cat", 4000L),
                new Event("wangwu", "/index", 1000L)
        );

        Properties kafkaPorperties = new Properties();
        kafkaPorperties.put("bootstrap.servers", "localhost:9092");
        kafkaPorperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaPorperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaPorperties.put("group.id", "jyong1");
        String topic = "wjy-test-topic";


        //3.输出到kafka
        eventDataStreamSource.map(Event::toString).addSink(new FlinkKafkaProducer<String>(topic, new SimpleStringSchema(), kafkaPorperties));

        //4.触发执行
        env.execute();


    }


}
