package com.jyong.flink.source;

import com.jyong.commons.conf.Constants;
import com.jyong.flink.sink.HbaseWriter;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class CustomKafkaStreaming {
    public static void main(String[] args) throws Exception {

        Properties kafkaPorperties = new Properties();
        kafkaPorperties.put("bootstrap.servers", Constants.KAFKA_BOOTSTRAP_SERVERS);
        kafkaPorperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaPorperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaPorperties.put("group.id", Constants.KAFKA_GROUP_ID);
        String topic = Constants.KAFKA_TOPIC;

        LocalStreamEnvironment environment = StreamExecutionEnvironment.createLocalEnvironment();

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), kafkaPorperties);
        DataStreamSource<String> streamSource = environment.addSource(kafkaConsumer);

        streamSource.print();
//        SingleOutputStreamOperator<String> streamOperator = streamSource.map(new MapFunction<String, String>() {
//            @Override
//            public String map(String s) throws Exception {
//
//                return "数据长度：" + s.length() * 10000;
//            }
//        });
//        //writetoHbase
//        streamOperator.addSink(new HbaseWriter());



        environment.execute();

    }


}
