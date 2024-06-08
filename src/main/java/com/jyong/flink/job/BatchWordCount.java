package com.jyong.flink.job;


import com.jyong.flink.entity.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class BatchWordCount {


    public static void main(String[] args) throws Exception {

        //创建执行坏境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //1.从文件中读取

        String path="D:\\codding\\Datawaiter\\Datawaiter-Flink-Streaming\\src\\main\\resources\\clicks.txt";
        DataStreamSource<String> fileStreamSource = env.readTextFile(path);

        //2.从集合中读取
        List<String> list = new ArrayList<>();
        list.add("1");
        list.add("2");
        list.add("3");
        DataStreamSource<String> collectionDataStream = env.fromCollection(list);

        ArrayList<Event> events = new ArrayList<>();
        events.add(new Event("mary","./cart",1000L));
        events.add(new Event("bob","./home",2000L));
        DataStreamSource<Event> eventDataStreamSource = env.fromCollection(events);


        fileStreamSource.print("fileStreamSource");

        collectionDataStream.print("collectionDataStream");

        eventDataStreamSource.print("eventDataStreamSource");

        env.execute();


    }


}
