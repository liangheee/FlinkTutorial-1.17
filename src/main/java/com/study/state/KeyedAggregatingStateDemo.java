package com.study.state;

import com.study.bean.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author Hliang
 * @create 2023-08-19 22:45
 */
public class KeyedAggregatingStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("hadoop102", 7777)
                .map(value -> {
                    String[] split = value.split(",");
                    return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                });

        // TODO 计算每种传感器的平均水位
        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(sensor -> sensor.getId());

        /**
         * TODO 分析
         *  1、输入类型为Integer vc
         *  2、聚合中间结果类型为 Tuple2，(vcSum,count)
         *  3、输出类型为Double，vc/count
         */
        sensorKS.process(new KeyedProcessFunction<String, WaterSensor, String>() {

            AggregatingState<Integer,Double> vcAvgAggregatingState;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                vcAvgAggregatingState = getRuntimeContext().getAggregatingState(
                        new AggregatingStateDescriptor<Integer, Tuple2<Integer,Integer>, Double>("vcAvgAggregatingState", new AggregateFunction<Integer, Tuple2<Integer,Integer>, Double>() {
                            @Override
                            public Tuple2<Integer, Integer> createAccumulator() {
                                return Tuple2.of(0,0);
                            }

                            @Override
                            public Tuple2<Integer, Integer> add(Integer value, Tuple2<Integer, Integer> accumulator) {
                                return Tuple2.of(accumulator.f0 + value,accumulator.f1 + 1);
                            }

                            @Override
                            public Double getResult(Tuple2<Integer, Integer> accumulator) {
                                return accumulator.f0 * 1D / accumulator.f1;
                            }

                            @Override
                            public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
                                return null;
                            }
                        }, Types.TUPLE(Types.INT, Types.INT))
                );
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                // 获取vc值
                Integer vc = value.getVc();
                // 添加vc到聚合状态中
                vcAvgAggregatingState.add(vc);
                // 获取聚合结果
                Double vcAvg = vcAvgAggregatingState.get();
                // 向下游传递输出
                out.collect("传感器id为" + value.getId() + ",平均水位值=" + vcAvg);

                // TODO aggregatingState总结
    //                vcAvgAggregatingState.get(); // 获取本分组 aggregating状态中 的数据
    //                vcAvgAggregatingState.add(); // 向本分组 aggregating状态中 添加数据
    //                vcAvgAggregatingState.clear(); // 清除 aggregating状态
            }
        }).print();



        env.execute();
    }
}
