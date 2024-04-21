package com.study.state;

import com.study.bean.WaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author Hliang
 * @create 2023-08-19 21:02
 */
public class KeyedReducingStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("hadoop102", 7777)
                .map(value -> {
                    String[] split = value.split(",");
                    return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                });

        // TODO 计算每种传感器的水位和
        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(sensor -> sensor.getId());

        sensorKS.process(new KeyedProcessFunction<String, WaterSensor, String>() {

            ReducingState<Integer> vcSumReducingState;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                /**
                 * 向归约状态的描述器传入三个参数
                 * 第一个参数：归约状态的唯一名称
                 * 第二个参数：归约状态的归约函数 （向归约状态中添加数据，就会调用该方法，第一条进来的数据不会调用该方法）
                 * 第三个参数：归约结果的类型  （归约状态的输入数据类型、中间结果类型、输出结果类型三者一致）
                 */
                vcSumReducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Integer>(
                        "vcReducingState",
                        new ReduceFunction<Integer>() {
                            @Override
                            public Integer reduce(Integer value1, Integer value2) throws Exception {
                                return value1 + value2;
                            }
                        }
                        , Types.INT)
                );
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                // 获取当前key下的水位线
                Integer vc = value.getVc();
                // 添加到归约状态中
                vcSumReducingState.add(vc);
                // 获取现在归约状态中的值
                Integer vcSum = vcSumReducingState.get();
                // 输出
                out.collect("传感器id为" + value.getId() + ",水位值总和=" + vcSum);



                 // TODO reducingState的操作总结
    //                vcSumReducingState.get(); // 获取本分组 Reducing状态 中的归约结果值
    //                vcSumReducingState.add(); // 向本分组 Reducing状态 中添加新的值进行归约
    //                vcSumReducingState.clear(); // 清除本分组 Reducing状态


            }
        }).print();


        env.execute();
    }
}
