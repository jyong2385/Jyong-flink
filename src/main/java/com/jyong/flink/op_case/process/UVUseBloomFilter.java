package com.jyong.flink.op_case.process;

import com.jyong.flink.op_case.beans.PageViewCount;
import com.jyong.flink.op_case.beans.UserBehavior;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * @Author jyong
 * @Date 2023/6/11 14:17
 * @desc 使用布隆过滤器，优化uv数据
 */

public class UVUseBloomFilter {
    private static final String DATA_PATH = "/Users/jyong/Desktop/jyong/workplace/coding/Jyong/UserBehaviorAnalysis/HotItemsAnalysis/src/main/resources/data/UserBehavior.csv";

    public static void main(String[] args) throws Exception {

//1.创建执行坏境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2.读取数据,创建DataStream数据流
        DataStreamSource<String> inputDataStream = env.readTextFile(DATA_PATH);

        //3.转换为POJO,分配事件戳和watermark
        SingleOutputStreamOperator<UserBehavior> dataStream = inputDataStream.map(data -> {
            String[] split = data.split(",");
            return new UserBehavior(Long.parseLong(split[0]), Long.parseLong(split[1]), Integer.parseInt(split[2]), split[3], Long.parseLong(split[4]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                    @Override
                    public long extractTimestamp(UserBehavior userBehavior, long l) {

                        return userBehavior.getTimestamp() * 1000;

                    }
                })
        );

        /**4.分组开窗聚合，得到每个窗口内各个商品的count值
         * 利用key by优化处理
         */
        dataStream
                //过滤掉不必要的数据,过滤掉pv行为
                .filter(new FilterFunction<UserBehavior>() {
                    @Override
                    public boolean filter(UserBehavior userBehavior) throws Exception {

                        return "pv".equals(userBehavior.getBehaivor());
                    }
                })
                .timeWindowAll(Time.hours(1))
                .trigger(new MyTrigger())
                .process(new UvCountResultWithBoolmFilter()).print();

        env.execute();

    }

    /**
     * 自定义定时器触发
     */
    private static class MyTrigger extends Trigger<UserBehavior, TimeWindow> {

        /**
         * 时间到达时，处理事件逻辑的方法
         *
         * @param element   The element that arrived.
         * @param timestamp The timestamp of the element that arrived.
         * @param window    The window to which the element is being added.
         * @param ctx       A context object that can be used to register timer callbacks.
         * @return
         * @throws Exception
         */
        @Override
        public TriggerResult onElement(UserBehavior element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            //每来一条数据，直接触发窗口计算，并且直接清空窗口
            return TriggerResult.FIRE_AND_PURGE;
        }

        /**
         * 处理时间定时器逻辑
         *
         * @param time   The timestamp at which the timer fired.
         * @param window The window for which the timer fired.
         * @param ctx    A context object that can be used to register timer callbacks.
         * @return
         * @throws Exception
         */
        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        /**
         * 事件时间定时器逻辑处理
         *
         * @param time   The timestamp at which the timer fired.
         * @param window The window for which the timer fired.
         * @param ctx    A context object that can be used to register timer callbacks.
         * @return
         * @throws Exception
         */
        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        /**
         * 清除窗口信息的操作
         *
         * @param window
         * @param ctx
         * @throws Exception
         */
        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

        }
    }

    /**
     * 自定义一个布隆过滤器
     */
    private static class CustomBloomFilter {

        //定义位图的大小,一般需要定义为2的整次幂
        private Integer cap;

        public CustomBloomFilter(Integer cap) {
            this.cap = cap;
        }

        //实现一个hash函数
        public Long hashCode(String value, Integer seed) {
            Long result = 0L;
            for (int i = 0; i < value.length(); i++) {
                result = result * seed + value.charAt(i);
            }
            return result & (cap - 1);
        }


    }

    //实现自定义处理函数
    private static class UvCountResultWithBoolmFilter extends ProcessAllWindowFunction<UserBehavior, PageViewCount, TimeWindow> {


        //定义jedis连接和布隆过滤器
        private Jedis jedis;
        private CustomBloomFilter customBloomFilter;

        @Override
        public void open(Configuration parameters) throws Exception {
            jedis = new Jedis("127.0.0.1", 6379);
            customBloomFilter = new CustomBloomFilter(1 << 29); //要处理1亿个数据，约需要64MB的位图数据
        }


        @Override
        public void process(ProcessAllWindowFunction<UserBehavior, PageViewCount, TimeWindow>.Context context, Iterable<UserBehavior> elements, Collector<PageViewCount> out) throws Exception {

            //将位图和count值全部存入redis,用windowEnd作为key
            Long windowEnd = context.window().getEnd();
            String bitmapKey = windowEnd.toString();
            //把count值存成一张hash表
            String countHashName = "uv_count";

            //1.取当前的userId
            Long userId = elements.iterator().next().getUserId();
            String countKey = new Timestamp(windowEnd).toString();

            //2.计算位图中的偏移量
            Long offset = customBloomFilter.hashCode(userId.toString(), 61);

            //3.用redis的getbit命令判断，对应位置的值
            boolean isExist = jedis.getbit(bitmapKey, offset);


            if (!isExist) {
                //如果不存在，对应位图置1
                jedis.setbit(bitmapKey, offset, true);

                //更新redis中保存的count值
                Long count = 0L;
                String uvCountStr = jedis.hget(countHashName, countKey);
                if (StringUtils.isNotBlank(uvCountStr)) {
                    count = Long.valueOf(uvCountStr);
                }
                jedis.hset(countHashName, countKey, String.valueOf(count + 1));

                out.collect(new PageViewCount("uv", windowEnd, count + 1));
            }
        }

        @Override
        public void close() throws Exception {
            jedis.close();
        }
    }


}
