package com.jyong.flink.job.flinksql;

import com.esotericsoftware.kryo.io.Output;
import com.jyong.flink.entity.Event;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.*;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.Set;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @Author jyong
 * @Date 2023/5/28 14:00
 * @desc 自定义函数UDF
 */

public class FlinkUDFExample {

    public static void main(String[] args) throws Exception {

        //1.创建执行坏境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.利用创建表方式介入数据源
        String createDDL = "CREATE TABLE clickTable( " +
                " `user` STRING, " +
                " url STRING, " +
                " ts BIGINT, " +
                " et AS TO_TIMESTAMP(FROM_UNIXTIME(ts / 1000)), " +
                " WATERMARK FOR et AS et - INTERVAL '1' SECOND " +
                ") WITH ( " +
                " 'connector' = 'filesystem'," +
                " 'path' = '/Users/jyong/Desktop/jyong/workplace/coding/Jyong/Jyong-flink/src/main/resources/clicks.txt'," +
                "  'format' = 'csv') ";

        tableEnv.executeSql(createDDL);


        /**
         * 3.注册自定义函数
         * 1.标量函数
         *  继承ScalarFunction类，并手动实现eval方法
         * 2.表函数
         *  继承TableFunction类，并手动实现eval方法
         * 3.聚合函数
         */

        //3.1 聚合函数

        //3.1.2 将函数注册到当前临时坏境
        // tableEnv.createTemporaryFunction("MyHashUDF",MyHashFunction.class);

        //3.1.3 应用并转换成了流打印输出
        //Table table = tableEnv.sqlQuery("select user,MyHashUDF(url) as le from clickTable ");
        //tableEnv.toDataStream(table).print();

        //3.2 表函数
        //3.2.2 注册临时系统函数-表函数
//        tableEnv.createTemporarySystemFunction("SplitFunction", SplitFunction.class);
//
//        //3.2.3 表函数应用并打印输出
//        Table table1 = tableEnv.sqlQuery(
//                "select user,url,word,length " +
//                        "from clickTable, LATERAL TABLE(SplitFunction(url)) as T(word,length)"
//        );
//
//        tableEnv.toDataStream(table1)
//                .print();

//        //3.3.2 注册聚合函数-表函数
//        tableEnv.createTemporarySystemFunction("MyAggFunction", MyAggFunction.class);
//
//        //3.3.3 应用并打印输出
//        Table table = tableEnv.sqlQuery("select user,MyAggFunction(ts,1) as w_avg from clickTable group by user");
//        tableEnv.toChangelogStream(table)
//                .print();

        //3.4.2 注册聚合函数-表聚合函数
        tableEnv.createTemporarySystemFunction("Top2F", Top2F.class);
//
//        //3.4.3 应用并打印输出
        //窗口TopN,统计一段时间内的前2名活跃用户
        String top2SubSql = "SELECT user,count(url) as cnt,window_start,window_end " +
                "FROM TABLE( " +
                " TUMBLE(TABLE clickTable,DESCRIPTOR(et),INTERVAL '10' SECOND) " +
                ") " +
                "GROUP BY user,window_start,window_end ";

        Table table = tableEnv.sqlQuery(top2SubSql);
        Table rankResult = table.groupBy($("window_end"))
                .flatAggregate(call("Top2F", $("cnt")).as("value", "rank"))
                .select($("window_end"), $("value"), $("rank"));


        tableEnv.toChangelogStream(rankResult)
                .print();

        //触发执行
        env.execute();

    }


    //3.1.1 实现自定义标量函数
    public static class MyHashFunction extends ScalarFunction {

        public int eval(String s) {
            return s.hashCode();
        }

    }

    //3.2.1 实现自定义表函数
    public static class SplitFunction extends TableFunction<Tuple2<String, Integer>> {
        public void eval(String s) {
            String[] split = s.split("\\?");
            for (String field : split) {
                collect(Tuple2.of(field, field.length()));
            }
        }

    }

    //单独定义一个累加器类型
    public static class WeightAvgAccumulator {
        public long sum = 0;
        public int count = 0;

    }

    //3.3.1 实现自定义聚合函数-实现加权平均值
    public static class MyAggFunction extends AggregateFunction<Long, WeightAvgAccumulator> {
        @Override
        public Long getValue(WeightAvgAccumulator accumulator) {
            if (accumulator.count == 0) {
                return null;
            } else {
                return accumulator.sum / accumulator.count;
            }
        }

        @Override
        public WeightAvgAccumulator createAccumulator() {
            return new WeightAvgAccumulator();
        }

        public void accumulate(WeightAvgAccumulator accumulator, Long iValue, Integer iWeight) {
            accumulator.sum += iValue * iWeight;
            accumulator.count += iWeight;
        }
    }

    //定义一个累加器
    //单独定义一个累加器类型
    public static class Top2Accumulator {
        private long max;
        private long secondMax;


        public long getMax() {
            return max;
        }

        public void setMax(long max) {
            this.max = max;
        }

        public long getSecondMax() {
            return secondMax;
        }

        public void setSecondMax(long secondMax) {
            this.secondMax = secondMax;
        }
    }

    //3.4.1 表聚合函数
    public static class Top2F extends TableAggregateFunction<Tuple2<Long, Integer>, Top2Accumulator> {
        @Override
        public Top2Accumulator createAccumulator() {
            Top2Accumulator top2Accumulator = new Top2Accumulator();
            top2Accumulator.setMax(Long.MAX_VALUE);
            top2Accumulator.setSecondMax(Long.MAX_VALUE);
            return top2Accumulator;
        }

        //定义一个更新累加器的方法
        public void accumulate(Top2Accumulator accumulator, Long value) {
            if (value > accumulator.getMax()) {
                accumulator.setSecondMax(accumulator.getSecondMax());
                accumulator.setMax(value);
            } else if (value > accumulator.getSecondMax()) {
                accumulator.setSecondMax(value);
            }

        }

        //输出结果
        public void emitValue(Top2Accumulator accumulator, Collector<Tuple2<Long, Integer>> out) {

            if (accumulator.getMax() != Long.MAX_VALUE) {
                out.collect(Tuple2.of(accumulator.max, 1));
            }
            if (accumulator.getSecondMax() != Long.MAX_VALUE) {
                out.collect(Tuple2.of(accumulator.secondMax, 2));
            }

        }

    }


}
