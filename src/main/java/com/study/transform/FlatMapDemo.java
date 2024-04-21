package com.study.transform;

import com.study.bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author Hliang
 * @create 2023-08-13 13:23
 */
public class FlatMapDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> sensorDS = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );

        /*
            TODO flatMap：一进多出
                对于s1的数据，一进一出
                对于s2的数据，一进二出
                对于s3的数据，一进零出（类似于过滤的效果）

                TODO map怎么控制的一进一出：使用return
                TODO flatMap怎么控制的一进多出：通过使用Collector采集器来向下游传递数据，调用几次collect就输出几条
         */

        SingleOutputStreamOperator<String> sensorFlatMappedDS = sensorDS.flatMap(new FlatMapFunction<WaterSensor, String>() {

            @Override
            public void flatMap(WaterSensor value, Collector<String> out) throws Exception {
                if ("s1".equals(value.getId())) {
                    // 如果是s1，就输出vc
                    out.collect(value.getVc().toString());
                } else if ("s2".equals(value.getId())) {
                    // 如果是s2，就输出ts和vc
                    out.collect(value.getTs().toString());
                    out.collect(value.getVc().toString());
                }
            }
        });

        sensorFlatMappedDS.print();

        env.execute();
    }
}
