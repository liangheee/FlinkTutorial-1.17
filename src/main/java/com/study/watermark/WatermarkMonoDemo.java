package com.study.watermark;

import com.study.bean.WaterSensor;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author Hliang
 * @create 2023-08-16 11:20
 */
public class WatermarkMonoDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // TODO flink1.17默认的时间语义就是 事件时间语义
//        env.setStreamTimeCharacteristic();
        // 摄取时间语义是指数据进入系统时，获得的当前的系统时间作为watermark，watermark值就是当前系统时间取整，currentTime - (currentTime % watermarkInterval)
        // IngestionTime不会因为数据流断流而导致watermark无法提升，如果对数据延迟不大，并且对窗口数据统计不严格的场景，而且可能出现断流的情况下，可以考虑使用ingestionTime时间语义
//        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 7777);

        SingleOutputStreamOperator<WaterSensor> sensorDS = socketTextStream.map(value -> {
            String[] split = value.split(",");
            return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
        });

        // 1、绑定watermark策略（分配时间戳和watermark   =》 进一步指定watermark中使用的时间戳来自于哪里）
        SingleOutputStreamOperator<WaterSensor> sensorDSWithWatermark = sensorDS.assignTimestampsAndWatermarks(
                // 1.1 指定watermark策略：升序的watermark，没有等待时间
                WatermarkStrategy.<WaterSensor>forMonotonousTimestamps()
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

        // TODO 分配窗口，必须是事件时间窗口EventTime
        SingleOutputStreamOperator<String> sensorWS = sensorKS.window(
                // TODO 使用了事件时间，一定要使用 事件时间语义 窗口
                TumblingEventTimeWindows.of(Time.seconds(10))
        ).process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
            @Override
            public void process(String s, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                String windowStart = DateFormatUtils.format(context.window().getStart(), "yyyy-MM-dd HH:mm:ss.SSS");
                String windowEnd = DateFormatUtils.format(context.window().getEnd(), "yyyy-MM-dd HH:mm:ss.SSS");
                long count = elements.spliterator().estimateSize();
                
                out.collect("key="+ s + "的窗口范围[" + windowStart + " - " + windowEnd +"]，共有" + count + "条数据，WaterSensor = " + elements);
            }
        });

        sensorWS.print();

        env.execute();
    }
}
