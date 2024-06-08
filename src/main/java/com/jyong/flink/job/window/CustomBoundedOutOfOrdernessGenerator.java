package com.jyong.flink.job.window;

import com.jyong.flink.entity.Event;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/**
 * @author: jyong
 * @description 自定义CustomBoundedOutOfOrdernessGenerator
 * @date: 2023/3/30 20:54
 */
public class CustomBoundedOutOfOrdernessGenerator implements WatermarkGenerator<Event> {

    //延迟时间
    private Long delayTime = 5000L;

    //观察到的最大时间
    private Long maxTs = -Long.MAX_VALUE + delayTime + 1L;

    @Override
    public void onEvent(Event event, long l, WatermarkOutput watermarkOutput) {
        //每来一次就调用一次,更新最大的时间戳
        maxTs = Math.max(event.getTimestamp(), maxTs);
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
        //发射水位线，默认200ms调用一次
        watermarkOutput.emitWatermark(new Watermark(maxTs - delayTime - 1L));
    }
}
