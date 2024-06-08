package com.jyong.flink.source;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author: jyong
 * @description flink消费socker数据
 * @date: 2023/3/17 21:07
 */
public class FlinkCustomSocket {

    public static void main(String[] args) throws Exception {

        //1.创建流式执行坏境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        //2.从参数中获取主机名和端口名
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");

        //3.读取文本流
        DataStream<String> lineDataStream = env.socketTextStream(host, port);

        //4.数据处理逻辑
        SingleOutputStreamOperator<Tuple2<String, Long>> outputStreamOperator = lineDataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                String[] split = s.split(" ");
                for (String word : split) {
                    collector.collect(Tuple2.of(word, 1L));
                }

            }
        });

        //5.打印输出
        outputStreamOperator.print();

        //6.触发执行
        env.execute();

    }


}
