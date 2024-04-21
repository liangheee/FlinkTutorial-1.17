package com.study.aggregate;

import com.study.bean.WaterSensor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Hliang
 * @create 2023-08-13 13:22
 */
public class KeyByDemo  {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<WaterSensor> sensorDS = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 11L, 11),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );


        /*
        TODO keyBy：将相同key的数据进行分组
            要点：
                1.返回的是一个keyedStream键控流
                2.keyBy不是 转换算子，只是对数据进行重分区，也就是对keyedStream不能设置并行度（setParallelism）
                3.分组 与 分区的关系
                    1）keyBy是对数据分组，保证相同key的数据 在同一个分区（分区《=》子任务）
                    2）分区：一个子任务可以理解为一个分区，一个分区（子任务）中可以存在多个分组
         */
        // KeySelector泛型：第一个泛型参数是被处理的数据的类型，第二个泛型参数是选出的key的类型
        KeyedStream<WaterSensor, String> keyedStream = sensorDS.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor value) throws Exception {
                return value.getId();
            }
        });

        keyedStream.print();


        env.execute();
    }
}
