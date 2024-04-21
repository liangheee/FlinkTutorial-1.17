package com.study.transform;

import com.study.bean.WaterSensor;
import com.study.function.WaterSensorMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Hliang
 * @create 2023-08-13 12:37
 */
public class MapDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> sensorDS = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );

        // TODO map算子：一进一出
        // TODO 方式一：匿名实现类
//        SingleOutputStreamOperator<String> sensorMappedDS = sensorDS.map(new MapFunction<WaterSensor, String>() {
//
//            @Override
//            public String map(WaterSensor value) throws Exception {
//                return value.getId();
//            }
//        });

        // TODO 方式二：lambda表达式
//        SingleOutputStreamOperator<String> sensorMappedDS = sensorDS.map(value -> value.getId());

        // TODO 方式三：定义一个类来实现MapFunction
        SingleOutputStreamOperator<String> sensorMappedDS = sensorDS.map(new WaterSensorMapFunction());

        sensorMappedDS.print();

        env.execute();
    }
}
