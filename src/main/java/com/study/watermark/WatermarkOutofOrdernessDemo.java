package com.study.watermark;

import com.study.bean.WaterSensor;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.KeyedStream;
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
 * @create 2023-08-17 17:30
 */
public class WatermarkOutofOrdernessDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);

        // TODO 设置并行度为2，观察在多并行度下的水位线传递
        env.setParallelism(2);

        // TODO 可以手动设置watermark的生成周期
//        env.getConfig().setAutoWatermarkInterval()

        SingleOutputStreamOperator<WaterSensor> socketTextStream = env.socketTextStream("hadoop102", 7777)
                .map(value -> {
                    String[] split = value.split(",");
                    return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                });
        /**
         * TODO 总结
         *  1、flink内部提供了两种内置的watermark：
         *          1）递增有序的watermark：无等待时间
         *          2）乱序的watermark：有等待时间，需要手动设置
         *  2、内置的watermark的生成原理
         *          1）有序的watermark：当前最大的时间戳 - 1ms
         *          2）乱序的watermark：当前最大的时间戳 - 等待时间 - 1ms
         *  3、watermark的生成时间：默认是200ms
         *          可以手动指定，通过 env.getConfig().setAutoWatermarkInterval(毫秒值)
         */

        // 1、给数据流分配时间戳和水位线  （主要指定水位线中的时间戳来自哪里）
        SingleOutputStreamOperator<WaterSensor> sensorDSWithWatermark = socketTextStream.assignTimestampsAndWatermarks(
                // 1.1 指定watermark策略：无序的watermark，有等待时间  (当前我们指定是3s)
                WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        // 1.2 分配时间戳，指定watermark中的时间戳来自哪里，单位 毫秒
                        .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                            /**
                             * 提取时间戳
                             * @param element 输入数据
                             * @param recordTimestamp 默认记录时间戳，为Long.minValue
                             * @return 返回的时间戳要是毫秒
                             */
                            @Override
                            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                System.out.println("数据=" + element + ",recordTs=" + recordTimestamp);
                                return element.getTs() * 1000L;
                            }
                        })
        );

        KeyedStream<WaterSensor, String> sensorKS = sensorDSWithWatermark.keyBy(sensor -> sensor.getId());

        // 2、指定窗口类型为 事件时间语义 的滚动窗口
        SingleOutputStreamOperator<String> sensorWS = sensorKS.window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        String windowStart = DateFormatUtils.format(context.window().getStart(), "yyyy-MM-dd HH:mm:ss.SSS");
                        String windowEnd = DateFormatUtils.format(context.window().getEnd(), "yyyy-MM-dd HH:mm:ss.SSS");

                        long count = elements.spliterator().estimateSize();

                        out.collect("key=" + s + "的窗口范围为[" + windowStart + "-" + windowEnd + "]，一共有" + count + "条数据，WaterSensor = " + elements);
                    }
                });

        sensorWS.print();

        env.execute();
    }
}
