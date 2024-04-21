package com.study.aggregate;

import com.study.bean.WaterSensor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Hliang
 * @create 2023-08-14 10:29
 */
public class SimpleAggregateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> sensorDS = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 11L, 11),
                new WaterSensor("s1", 9L, 11),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );


        /*
        TODO 简单聚合算子：sum/max/min/minBy/maxBy
            要点：
                1.在调用简单聚合算子前，必须进行keyBy操作，转换为键控流
                2.是分组内的聚合：对于相同key的数据进行聚合
         */
        KeyedStream<WaterSensor, String> keyedStream = sensorDS.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor value) throws Exception {
                return value.getId();
            }
        });

        // TODO 对于POJO类型，不能通过基于位置索引的方式来选择聚合字段
//        SingleOutputStreamOperator<WaterSensor> result = keyedStream.sum(2);
//        SingleOutputStreamOperator<WaterSensor> result = keyedStream.sum("vc");


        /*
        TODO max和maxBy的区别  （min和minBy同理）
            max：只会取比较字段值的最大值，非比较字段保留第一次的取值
            maxBy：只会取比较字段的最大值，非比较字段取 比较字段最大值第一次出现时 的值
         */
//        SingleOutputStreamOperator<WaterSensor> result = keyedStream.max("vc");
        SingleOutputStreamOperator<WaterSensor> result = keyedStream.maxBy("vc");

        result.print();

        env.execute();
    }
}
