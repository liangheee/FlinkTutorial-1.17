package com.study.watermark;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

import java.time.Duration;

/**
 * @author Hliang
 * @create 2023-08-17 18:19
 */
public class MyPuntuatedWatermarkGenerator<T> implements WatermarkGenerator<T> {

    private long maxTs;
    private long outOfOrdernessTimestamp;

    public MyPuntuatedWatermarkGenerator(Duration maxOutOfOrderness){
        this.outOfOrdernessTimestamp = maxOutOfOrderness.toMillis();
        this.maxTs = Long.MIN_VALUE + this.outOfOrdernessTimestamp + 1;
    }

    // TODO 直接在这里进行断点是触发发送watermark，来一条数据计算生成一个watermark
    @Override
    public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
        this.maxTs = Math.max(eventTimestamp,this.maxTs);
        output.emitWatermark(new Watermark(this.maxTs - this.outOfOrdernessTimestamp - 1));
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {

    }
}
