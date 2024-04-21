package com.study.watermark;

import com.study.bean.WaterSensor;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author Hliang
 * @create 2023-08-17 17:51
 */
public class WatermarkCustomDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("hadoop102", 7777)
                .map(value -> {
                    String[] split = value.split(",");
                    return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                });

//        SingleOutputStreamOperator<WaterSensor> sensorDSWithWatermark = sensorDS.assignTimestampsAndWatermarks(
//                // TODO 手动指定自定义的watermark策略
//                WatermarkStrategy.<WaterSensor>forGenerator(new WatermarkGeneratorSupplier<WaterSensor>() {
//                    @Override
//                    public WatermarkGenerator<WaterSensor> createWatermarkGenerator(Context context) {
//                 // TODO 指定周期性式watermark
//                        return new MyPeriodWatermarkGenerator<WaterSensor>(Duration.ofSeconds(3));
//                    }
//                }).withTimestampAssigner((watersensor,recordTimestamp) -> watersensor.getTs() * 1000L)
//        );

        SingleOutputStreamOperator<WaterSensor> sensorDSWithWatermark = sensorDS.assignTimestampsAndWatermarks(
                // TODO 手动指定自定义的watermark策略
                WatermarkStrategy.<WaterSensor>forGenerator(new WatermarkGeneratorSupplier<WaterSensor>() {
                    @Override
                    public WatermarkGenerator<WaterSensor> createWatermarkGenerator(Context context) {
                        // TODO 指定的断点式watermark
                        return new MyPuntuatedWatermarkGenerator<>(Duration.ofSeconds(3));
                    }
                }).withTimestampAssigner((watersensor,recordTimestamp) -> watersensor.getTs() * 1000L)
        );

        sensorDSWithWatermark.keyBy(sensor -> sensor.getId())
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        String windowStart = DateFormatUtils.format(context.window().getStart(), "yyyy-MM-dd HH:mm:ss.SSS");
                        String windowEnd = DateFormatUtils.format(context.window().getEnd(), "yyyy-MM-dd HH:mm:ss.SSS");

                        long count = elements.spliterator().estimateSize();

                        out.collect("key=" + s + "的窗口范围为[" + windowStart + "-" + windowEnd + "]，一共有" + count + "条数据，WaterSensor = " + elements);
                    }
                }).print();

        env.execute();
    }
}
