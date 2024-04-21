package com.study.watermark;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.util.Preconditions;

import java.time.Duration;

/**
 * @author Hliang
 * @create 2023-08-17 17:59
 */
public class MyPeriodWatermarkGenerator<T> implements WatermarkGenerator<T> {

    // maxTs 是事件时间，而不是目前水位线的中的时间戳，水位线中的时间戳需要进行计算，刚开始默认为Long的最小值
    private long maxTs;
    private long outOfOrdernessMillis;

    public MyPeriodWatermarkGenerator(Duration maxOutOfOrderness){
        Preconditions.checkNotNull(maxOutOfOrderness,"乱序等待时间为空");
        Preconditions.checkArgument(maxOutOfOrderness.isNegative(),"乱序等待时间必须大于0");
        this.outOfOrdernessMillis = maxOutOfOrderness.toMillis();
        this.maxTs = Long.MIN_VALUE + outOfOrdernessMillis + 1;
    }

    /**
     * 每一条数据来了都会触发这个方法
     * @param event 数据
     * @param eventTimestamp 数据中的时间戳
     * @param output 输出器
     */
    @Override
    public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
        this.maxTs = Math.max(this.maxTs,eventTimestamp);
    }

    /**
     * 周期性的发送水位线
     * @param output 输出器
     */
    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        output.emitWatermark(new Watermark(this.maxTs - this.outOfOrdernessMillis - 1));
    }
}
