package com.study.watermark;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Hliang
 * @create 2023-08-17 19:39
 */
public class WatermarkFromSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 从数据源中获取dataStream的时候就可以指定watermark策略
        //  不过要注意的是，在这里就不用指定水位线中的时间戳的来源了，flink会自动去数据源中提取
        //   同时要注意，这里制定了水位线策略后，不能再通过assignTimestampAndWatermark分配水位线，它们二者只能取一
//        env.fromSource(kafkaSource, WatermarkStrategy.<String>forMonotonousTimestamps(),"kafkaSource")

        env.execute();
    }
}
