package com.jyong.flink.job.flinksql;

import com.jyong.flink.entity.Event;
import com.jyong.flink.source.ClickSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

/**
 * @Author jyong
 * @Date 2023/5/21 16:29
 * @desc flink sql中支持的数据类型
 *
 * （1）原子类型
 *      基础数据类型Integer Double String 和通用数据类型
 * （2）Tuple类型
 *      当不对原子类型进行重命名时，默认的字段名就是"f0"
 * （3）POJO类型
 * （4）ROW类型
 *
 */

public class FlinkDataTypeTypeExample {

    public static void main(String[] args) {

        //1.1.利用流式坏境创建
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        //原子类型
        DataStreamSource<Integer> dataStreamSource1 = env.fromElements(1, 2, 3);
        Table table1 = tableEnv.fromDataStream(dataStreamSource1);

        //tuple类型
        DataStreamSource<Tuple2<String, Integer>> tuple2DataStreamSource = env.fromElements(
                Tuple2.of("zhangsan", 1),
                Tuple2.of("lisi", 2)
        );
        Table table2 = tableEnv.fromDataStream(tuple2DataStreamSource);

        //pojo类型
        DataStreamSource<Event> eventDataStreamSource = env.addSource(new ClickSource());
        Table table3 = tableEnv.fromDataStream(eventDataStreamSource);

        //row类型
        /**
         * RowKind：
         *  INSERT： 插入操作
         *  UPDATE_BEFORE：更新前的数据
         *  UPDATE_AFTER： 更新后的数据
         */
        DataStreamSource<Row> rowDataStreamSource = env.fromElements(
                Row.ofKind(RowKind.INSERT,  "alice", 12),
                Row.ofKind(RowKind.INSERT, "bob", 1),
                Row.ofKind(RowKind.UPDATE_BEFORE, "alice", 12),
                Row.ofKind(RowKind.UPDATE_AFTER,  "alice", 120)
                );

        tableEnv.fromDataStream(rowDataStreamSource);


    }

}
