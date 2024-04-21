package com.study.split;

import com.study.bean.WaterSensor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author Hliang
 * @create 2023-08-14 11:41
 */
public class SplitBySideOutputDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 7777);

        /**
         * TODO 使用侧输出流 实现分流
         * TODO 需求： Watersensor的数据，s1、s2的数据分别分开
         *
         * TODO 总结步骤：
         *      1.使用process底层处理算子
         *      2.定义OutputTag标签
         *      3.采用上下文对昂Context的output方法
         *      4.通过主流获取侧输出流
         */
        // 定义OutputTag标签
        // OutputTag对象参数解读
        // 第一个参数：定义输出标签的id
        // 第二个参数：定义输出标签中最终输出类型
        OutputTag<WaterSensor> s1OutputTag = new OutputTag<>("s1", Types.POJO(WaterSensor.class));
        OutputTag<WaterSensor> s2OutputTag = new OutputTag<>("s2", Types.POJO(WaterSensor.class));
        SingleOutputStreamOperator<WaterSensor> process = socketDS.process(new ProcessFunction<String, WaterSensor>() {
            @Override
            public void processElement(String value, Context ctx, Collector<WaterSensor> out) throws Exception {
                String[] split = value.split(" ");
                WaterSensor waterSensor = new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                if ("s1".equals(waterSensor.getId())) {
                    // 如果是s1，则输出到侧输出流中
                    ctx.output(s1OutputTag, waterSensor);
                } else if ("s2".equals(waterSensor.getId())) {
                    // 如果是s2，则输出到侧输出流中
                    ctx.output(s2OutputTag, waterSensor);
                } else {
                    // 如果非s1和s2，则输出到主流中
                    out.collect(waterSensor);
                }
            }
        });

        // 输出主流数据
        process.print("非s1和s2的数据");

        // 获取s1侧输出流数据
        SideOutputDataStream<WaterSensor> s1SideOutput = process.getSideOutput(s1OutputTag);
        SideOutputDataStream<WaterSensor> s2SideOutput = process.getSideOutput(s2OutputTag);

        s1SideOutput.printToErr("s1侧输出流数据");
        s2SideOutput.printToErr("s2侧输出流数据");

        env.execute();
    }
}
