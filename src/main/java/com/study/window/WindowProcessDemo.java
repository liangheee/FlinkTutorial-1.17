package com.study.window;

import com.study.bean.WaterSensor;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author Hliang
 * @create 2023-08-15 18:14
 */
public class WindowProcessDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 7777);
        SingleOutputStreamOperator<WaterSensor> sensorDS = socketTextStream.map(value -> {
            String[] split = value.split(",");
            return new WaterSensor(split[0],Long.parseLong(split[1]),Integer.parseInt(split[2]));
        });

        KeyedStream<WaterSensor, String> sensorKDS = sensorDS.keyBy(sensor -> sensor.getId());

        // TODO 以滚动时间窗口为例子
        WindowedStream<WaterSensor, String, TimeWindow> sensorWS = sensorKDS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        /**
         * TODO ProcessFunction的泛型解析：
         *      *      1）第一个泛型表示输入数据类型
                *      2）第二个泛型表示输出数据类型
                *      3）第三个泛型表示分组key的数据类型
                *      4）第四个泛型表示窗口类型
                */
        SingleOutputStreamOperator<String> process = sensorWS.process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
            /**
             *
             * @param key 分组的key
             * @param context 环境上下文对象，用于获取一些环境参数信息，比如窗口对象，用于测输出流等
             * @param elements 全窗口数据缓存
             * @param out 采集器，向下游采集传输数据
             * @throws Exception
             */
            @Override
            public void process(String key, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                TimeWindow window = context.window();
                
                String windowStart = DateFormatUtils.format(window.getStart(), "yyyy-MM-dd HH:mm:ss.SSS");
                String windowEnd = DateFormatUtils.format(window.getEnd(), "yyyy-MM-dd HH:mm:ss.SSS");
                long count = elements.spliterator().estimateSize();
                out.collect("key=" + key + "的窗口[" + windowStart + "," + windowEnd + ")包含" + count + "条数据===>" + elements.toString());
            }
        });

        /**
         * TODO process计算逻辑要点
         *          1）全窗口函数计算：数据来了，并不会触发ProcessWindowFunction中的process计算逻辑
         *          2）触发窗口结束后，才会调用process计算逻辑，并向采集collector.collect()向下游传递数据
         */
        process.print();

        env.execute();
    }
}
