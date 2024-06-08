package com.jyong.flink.sink;

import com.jyong.flink.entity.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @author: jyong
 * @description 输出redis
 * @date: 2023/3/26 19:45
 */
public class SinkToRedis {


    public static void main(String[] args) throws Exception {
        //1.创建流式执行坏境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        //2.从元素中读取数据
        DataStreamSource<Event> eventDataStreamSource = env.fromElements(new Event("zhangsan", "/index", 1000L),
                new Event("lisi", "/cat", 1000L),
                new Event("zhangsan", "/cat", 1000L),
                new Event("zhangsan", "/cat", 2000L),
                new Event("lisi", "/cat", 3000L),
                new Event("tianliu", "/cat", 4000L),
                new Event("wangwu", "/index", 1000L)
        );


        //3.创建redisSink
        //3.1创建jedis连接
        FlinkJedisPoolConfig jedisPoolConfig = new FlinkJedisPoolConfig.Builder()
                .setHost("localhost")
                .setPort(6379)
                .build();

        //3.3注入sink
        eventDataStreamSource.addSink(new RedisSink<>(jedisPoolConfig,new MyRedisMapper()));

        //4.触发执行
        env.execute();

    }


    //3.2 自定义RedisMapper
    private static class MyRedisMapper implements RedisMapper<Event> {
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET,"FLINK_REDIS_PREFIX");
        }

        @Override
        public String getKeyFromData(Event event) {
            return event.getUser();
        }

        @Override
        public String getValueFromData(Event event) {
            return event.toString();
        }
    }
}
