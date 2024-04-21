package com.study.process;

import com.study.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author Hliang
 * @create 2023-08-18 16:31
 */
public class KeyedProcessTimerDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("localhost", 7777)
                .map(value -> {
                    String[] split = value.split(",");
                    return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                }).assignTimestampsAndWatermarks(
                        WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((w, recordTimestamp) -> w.getTs() * 1000)
                );

        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(sensor -> sensor.getId());

        sensorKS.process(new KeyedProcessFunction<String, WaterSensor, String>() {
            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                // 获取当前数据的key
                String currentKey = ctx.getCurrentKey();

                // 获取当前数据的内部时间戳  数据中提取出来的事件时间  （如果没有分配指定，那么就是null）
                Long currentEventTime = ctx.timestamp();

                // 获取定时器服务
                TimerService timerService = ctx.timerService();

                // 1、事件时间案例
                timerService.registerEventTimeTimer(5000L);
                long currentWatermark = timerService.currentWatermark();
                System.out.println("当前key=" + currentKey + ",当前事件时间=" + currentEventTime + ",注册了一个5s的事件时间语义的定时器");
                System.out.println("当前的水位线=" + currentWatermark);

                // 获取当前的水位线
//                long currentWatermark = timerService.currentWatermark();
                // 获取当前的处理时间
//                long processingTime = timerService.currentProcessingTime();
                // 注册事件时间定时器、处理时间定时器
//                timerService.registerEventTimeTimer(); // 以毫秒为单位
//                timerService.registerProcessingTimeTimer(); // 以毫秒为单位

                // 一般来说删除很少使用
                // 删除事件时间定时器、处理时间定时器
//                timerService.deleteEventTimeTimer();
//                timerService.deleteProcessingTimeTimer();



            }

            /**
             * 定时器触发后调用该方法
             * @param timestamp
             * @param ctx
             * @param out
             * @throws Exception
             */
            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                super.onTimer(timestamp, ctx, out);
                String currentKey = ctx.getCurrentKey();

                System.out.println("key=" + currentKey + "现在时间是" + timestamp + "定时器触发");


                // 获取当前数据的所属key
//                String currentKey = ctx.getCurrentKey();

                // 获取当前定时器的时间语义是 事件时间 还是 处理时间
//                ctx.timeDomain()

            }
        }).print();


        env.execute();
    }
}
