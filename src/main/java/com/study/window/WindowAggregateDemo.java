package com.study.window;

import com.study.bean.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
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
public class WindowAggregateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 7777);
        SingleOutputStreamOperator<WaterSensor> sensorDS = socketTextStream.map(value -> {
            String[] split = value.split(",");
            return new WaterSensor(split[0],Long.parseLong(split[1]),Integer.parseInt(split[2]));
        });

        KeyedStream<WaterSensor, String> sensorKDS = sensorDS.keyBy(sensor -> sensor.getId());

        WindowedStream<WaterSensor, String, TimeWindow> sensorWS = sensorKDS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        /**
         * TODO aggregate计算逻辑要点：
         *      1）输入数据类型、中间数据类型、输出数据类型三者可以不同，灵活度更高
         *      2）属于本窗口的第一条数据进来的时候，会调用createAccumulator方法创建累加器并初始化累加器的值
         *      3）增量聚合，来一条数据就会调用一次add方法
         *      4）窗口触发的时候，会调用一次getResult方法
         */
        // TODO AggregateFunction的泛型解析：
        //  第一个泛型表示输入数据的类型；
        //  第二个泛型表示中间数据的类型（累加器的类型）；
        //  第三个泛型表示最终输出的类型
        SingleOutputStreamOperator<String> aggregate = sensorWS.aggregate(new AggregateFunction<WaterSensor, Integer, String>() {

            /**
             * 创建累加器，并初始化累加其中的值
             * TODO 窗口中出现第一条数据时调用一次该方法
             * @return
             */
            @Override
            public Integer createAccumulator() {
                System.out.println("创建累加器Acc，并初始化累加器的值为0");
                return 0;
            }

            /**
             * 将数据与累加器中的值进行运算
             * TODO 每进来一条数据，就会调用一次add方法
             * @param value 流入窗口的数据
             * @param accumulator 累加器中的值
             * @return
             */
            @Override
            public Integer add(WaterSensor value, Integer accumulator) {
                System.out.println("add方法调用了,WaterSensor=" + value + "，accumulator=" + accumulator);
                return value.getVc() + accumulator;
            }

            /**
             * TODO 触发窗口后调用一次该方法
             * @param accumulator 累加器中的值
             * @return
             */
            @Override
            public String getResult(Integer accumulator) {
                return "输出结果=" + accumulator.toString();
            }

            /**
             * merge会在GlobalWindow中使用到，在其它的窗口分配器中不会使用到，所以一般默认不管
             * @param a
             * @param b
             * @return
             */
            @Override
            public Integer merge(Integer a, Integer b) {
                return null;
            }
        });

        aggregate.print();


        env.execute();
    }
}
