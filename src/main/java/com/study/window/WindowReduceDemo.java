package com.study.window;

import com.study.bean.WaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @author Hliang
 * @create 2023-08-15 17:56
 */
public class WindowReduceDemo {
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
        // TODO reduce计算逻辑要点
        // TODO     1）必须是keyBy后的数据流窗口
        // TODO     2）WindowReduceDemo
        SingleOutputStreamOperator<WaterSensor> reduce = sensorWS.reduce(new ReduceFunction<WaterSensor>() {
            @Override
            public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                System.out.println("value1=" + value1 + "，value2=" + value2);
                return new WaterSensor(value1.getId(), value2.getTs(), value1.getVc() + value2.getVc());
            }
        });

        // TODO reduce计算逻辑结果要点：
        // TODO     1）第一条数据来了才会触发窗口的建立
        // TODO     2）每个窗口第一条数据来了不会进行计算reduce，会将其存起来
        // TODO     3）每个窗口从第二条数据来了，会对每条数据进行计算得到中间结果，并将中间结果存起来，不会进行输出
        // TODO     4）触发窗口后，输出窗口的计算结果
        reduce.print();


        env.execute();
    }
}
