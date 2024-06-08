package com.jyong.flink.job.flinksql;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @Author jyong
 * @Date 2023/5/21 15:35
 * @desc 表和流的转换
 */

public class TransferTableOrStreamExample {


    public static void main(String[] args) throws Exception {

        //1 基于blink计划器定义坏境
        //1.1.利用流式坏境创建
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        //2.1创建输入表
        String createDDL = "create table clickTable(" +
                " user_name string," +
                " url string," +
                " ts bigint " +
                ") with (" +
                " 'connector' = 'filesystem'," +
                " 'path' = '/Users/jyong/Desktop/jyong/workplace/coding/Jyong/Jyong-flink/src/main/resources/clicks.txt'," +
                " 'format' = 'csv' " +
                ")";
        tableEnv.executeSql(createDDL);


        Table table = tableEnv.sqlQuery("select * from clickTable");


        //todo 表转流
        tableEnv.toDataStream(table).print("to stream1");

        //2.聚合操作的表转流
        Table sqlQueryResult = tableEnv.sqlQuery("select user_name,count(url) as cnt from clickTable group by user_name");
        //聚合方法不支持：toDataStream
        /**
         * tableEnv.toDataStream(sqlQueryResult)
         *
         * Exception:
         * Table sink 'default_catalog.default_database.Unregistered_DataStream_Sink_2'
         * doesn't support consuming update changes which is produced by node GroupAggregate(groupBy=[user_name],
         * select=[user_name, COUNT(url) AS cnt])
         *
         */
//        tableEnv.toDataStream(sqlQueryResult).print();
        //需要用更新日志流
        DataStream<Row> stream = tableEnv.toChangelogStream(sqlQueryResult);
        stream.print("to stream 2");



        //todo 流转表
        Table fromDataStream = tableEnv.fromDataStream(stream);
        tableEnv.createTemporaryView("from_stream",fromDataStream);


        env.execute();


    }


}
