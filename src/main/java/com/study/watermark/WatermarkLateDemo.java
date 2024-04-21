package com.study.watermark;

import com.study.bean.WaterSensor;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @author Hliang
 * @create 2023-08-17 20:28
 */
public class WatermarkLateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("hadoop102", 7777)
                .map(value -> {
                    String[] split = value.split(",");
                    return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                });

        SingleOutputStreamOperator<WaterSensor> sensorDSWithWatermark = sensorDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((watersensor, recordTimeStamp) -> watersensor.getTs() * 1000)
        );

        /**
         * TODO 总结
         *  1、乱序与迟到的区别
         *      乱序：事件时间较早的数据比事件时间较晚的数据 来得更晚
         *      迟到：当前数据的事件时间 比当前的Watermark更早
         *  2、乱序、迟到数据的处理
         *      1）采用flink内置的forBoundedOutOfOrderness水位线策略
         *      2）Watermark中指定 乱序等待的时间
         *      3）如果开窗，设置窗口的迟到时间
         *              =》推迟关窗的时间，在关窗之前，迟到数据来了，来一条迟到数据触发一次计算，输出一次计算结果
         *              =》关窗后，迟到数据不会被计算
         *      4）关窗后的迟到数据，放入侧输出流
         *  3、如果Watermark等待3s，窗口允许迟到时间为2s，为什么不直接watermark等待5s 或者 窗口允许迟到5s？
         *      生产经验如下：
         *      =》 Watermark等待时间不会设置太大 ==》 太大会影响计算延迟
         *              如果乱序等待时间为3s ==》 窗口第一次触发计算和输出，13s的数据来。 13 - 3 = 10s
         *              如果乱序等待时间为5s ==》 窗口第一次触发计算和输出，15s的数据来。 15- 5 = 10s
         *      =》 窗口允许迟到，是对大部分迟到数据的处理，尽量让结果准确 （做不到精确）
         *              如果只设置 允许迟到5s，那么就会导致频繁的计算和输出  （因为可能之前部分乱序的数据也会来到这里输出）
         *    TODO 生产环境下的设置经验：
         *      1）Watermark乱序等待时间，设置一个不算特别大的，一般是秒级，在 乱序 和 延迟中 取舍
         *      2）设置一定的窗口允许迟到，只考虑大部分的迟到数据，极端小部分迟到很久的数据，不用去管
         *      3）极端小部分迟到很久的数据，放到侧输出流。获取到之后可以做各种处理
         */
        OutputTag<WaterSensor> outputTag = new OutputTag<>("late-data", Types.POJO(WaterSensor.class));
        SingleOutputStreamOperator<String> process = sensorDSWithWatermark.keyBy(sensor -> sensor.getId())
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                // TODO 设置窗口允许推迟关闭2s
                .allowedLateness(Time.seconds(2))
                // TODO 关窗后，迟到的数据放入侧输出流
                .sideOutputLateData(outputTag)
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        String windowStart = DateFormatUtils.format(context.window().getStart(), "yyyy-MM-dd HH:mm:ss.SSS");
                        String windowEnd = DateFormatUtils.format(context.window().getEnd(), "yyyy-MM-dd HH:mm:ss.SSS");

                        long count = elements.spliterator().estimateSize();

                        out.collect("key=" + s + "的窗口范围为[" + windowStart + "-" + windowEnd + "]，一共有" + count + "条数据，WaterSensor = " + elements);

                    }
                });

        // 打印主流
        process.print();
        // 打印测输出流
        process.getSideOutput(outputTag).printToErr();

        env.execute();
    }
}
