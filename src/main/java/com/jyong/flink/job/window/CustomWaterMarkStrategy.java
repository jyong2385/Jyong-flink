package com.jyong.flink.job.window;

import com.jyong.flink.entity.Event;
import org.apache.flink.api.common.eventtime.*;

import java.time.Duration;

/**
 * @author: jyong
 * @description 自定义watermark
 * @date: 2023/3/30 20:50
 */
public class CustomWaterMarkStrategy implements WatermarkStrategy<Event> {


    @Override
    public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        //到用自定义的watermarkGenerator
        return new CustomBoundedOutOfOrdernessGenerator();
    }

    //需要获取处理时间
    @Override
    public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return new SerializableTimestampAssigner<Event>() {
            @Override
            public long extractTimestamp(Event event, long l) {
                return event.getTimestamp();
            }
        };
    }

    @Override
    public WatermarkStrategy<Event> withTimestampAssigner(TimestampAssignerSupplier<Event> timestampAssigner) {
        return WatermarkStrategy.super.withTimestampAssigner(timestampAssigner);
    }

    @Override
    public WatermarkStrategy<Event> withTimestampAssigner(SerializableTimestampAssigner<Event> timestampAssigner) {
        return WatermarkStrategy.super.withTimestampAssigner(timestampAssigner);
    }

    @Override
    public WatermarkStrategy<Event> withIdleness(Duration idleTimeout) {
        return WatermarkStrategy.super.withIdleness(idleTimeout);
    }
}
