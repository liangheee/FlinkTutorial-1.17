package com.study.state;

import com.study.bean.WaterSensor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
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
public class StateTTLDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("hadoop102", 7777)
                .map(value -> {
                    String[] split = value.split(",");
                    return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                });

        // TODO 状态的TTL时间到了，并没有清除该状态，只是给该状态标记了不可见

        // TODO 1、创建状态存活配置对象StateTtlConfig
        //      指定过期时间、TTL更新类型（onCreateAndWrite 或者 OnReadAndWrite）、状态的可见性（NeverReturnExpired 或者 ReturnExpiredIfNotCLeanUp）
        //      默认更新类型为创建或修改时更新，默认状态可见性为过期状态不可以见
        StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.seconds(5))
//                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite) // 默认采用这种更新策略，创建和写状态的时候更新TTL
                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite) // 读取和写状态的时候更新TTL
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired) // 两种状态可见性（当状态过期之后是否可见）：不可见过期状态和可见过期状态
                .build();

        // TODO 验证状态TTL
        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(sensor -> sensor.getId());

        sensorKS.process(new KeyedProcessFunction<String, WaterSensor, String>() {

            ValueState<Integer> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                // TODO 创建状态的描述器，开启状态的存活配置
                ValueStateDescriptor<Integer> valueStateDescriptor = new ValueStateDescriptor<>("valueState", Types.INT);
                valueStateDescriptor.enableTimeToLive(stateTtlConfig);
                this.valueState = getRuntimeContext().getState(valueStateDescriptor);

            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                // 获取状态的值
                Integer lastVc = valueState.value();
                out.collect("key=" + value.getId() + ",状态值=" + lastVc);

                // 更新状态的值
//                valueState.update(Integer.valueOf(value.getVc()));
                // 如果水位大于10，更新状态值 ===》 写入状态
                if(value.getVc() > 10){
                    valueState.update(Integer.valueOf(value.getVc()));
                }

            }
        }).print();

        env.execute();
    }
}
