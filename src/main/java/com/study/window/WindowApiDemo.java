package com.study.window;

import com.study.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @author Hliang
 * @create 2023-08-14 23:26
 */
public class WindowApiDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 7777);
        SingleOutputStreamOperator<WaterSensor> sensorDS = socketTextStream.map(value -> {
            String[] split = value.split(",");
            return new WaterSensor(split[0],Long.parseLong(split[1]),Integer.parseInt(split[2]));
        });

        KeyedStream<WaterSensor, String> sensorKDS = sensorDS.keyBy(sensor -> sensor.getId());

        // TODO 核心的一点：flink是事件驱动型的，必须有数据来了，我们才会开启窗口

        // TODO 指定窗口分配器：指定使用哪一种窗口  时间 or 计数？ 滚动、滑动、会话？
        // TODO 1、非keyBy的数据流，我们只能进行windowAll操作，强制让并行度变成了1，所有的数据只能进入同一个子任务
//        sensorDS.windowAll()
//        sensorDS.countWindowAll()
        // TODO 2、有keyBy的数据流：每个key上都定义了一组窗口，各自独立的进行统计计算
        // TODO 2.1 基于时间的窗口
//        sensorKDS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));  // 滚动窗口，窗口长度为10s，各个窗口之间不重叠（是窗口大小和滑动步长一致的滑动窗口的特例）
//        sensorKDS.window(SlidingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(5))); // 滑动窗口，窗口长度为10s，滑动步长为5s，相邻的窗口之间会重叠
//        sensorKDS.window(ProcessingTimeSessionWindows.withGap(Time.seconds(10))); // 会话窗口，设置会话间隔时间为10s，数据间隔接收10s后，重新开启新的会话窗口，多个会话窗口之间不会重叠


        // TODO 2.2 基于计数的窗口
        sensorKDS.countWindow(5); // 滚动窗口，窗口长度为5个数据元素
//        sensorKDS.countWindow(5,2); // 滑动窗口，窗口长度为5个数据元素，滑动步长为2个数据元素
//        sensorKDS.window(GlobalWindows.create()); // 全局窗口，一直打开，除非我们自定义的触发器触发才会结束窗口

        // TODO 3、指定 窗口函数：窗口内数据的计算逻辑
        // TODO       1）增量聚合函数 reduce、aggregate
        // TODO       2）全窗口函数 process
        WindowedStream<WaterSensor, String, TimeWindow> sensorWS = sensorKDS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));
        // TODO 增量聚合：来一条数据，计算一条数据，窗口触发的时候输出计算结果
//        sensorWS.reduce();
//        sensorWS.aggregate();
        // TODO 全窗口函数：数据来了不计算，存起来，窗口触发的时候，进行数据计算，并输出计算结果
//        sensorWS.process();   // apply已经是旧API方法了，不推荐使用


        env.execute();

    }
}
