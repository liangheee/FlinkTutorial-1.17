package com.study.state;

import com.study.bean.WaterSensor;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class BroadcastStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // TODO 创建数据流
        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("localhost", 7777)
                .map(value -> {
                    String[] split = value.split(",");
                    return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                });

        // TODO 创建配置流
        SingleOutputStreamOperator<Integer> configDS = env.socketTextStream("localhost", 8888)
                .map(value -> Integer.parseInt(value));

        // TODO 获取广播流
        MapStateDescriptor<String, Integer> mapStateDescriptor = new MapStateDescriptor<>("broadcast-state", Types.STRING, Types.INT);
        BroadcastStream<Integer> configBCS = configDS.broadcast(mapStateDescriptor);

        BroadcastConnectedStream<WaterSensor, Integer> connectDS = sensorDS.connect(configBCS);
        connectDS.process(new BroadcastProcessFunction<WaterSensor, Integer, String>() {
            @Override
            public void processElement(WaterSensor value, BroadcastProcessFunction<WaterSensor, Integer, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
                ReadOnlyBroadcastState<String, Integer> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                Integer threshold = broadcastState.get("threshold");
                threshold = (threshold == null)? threshold = 0 : threshold;
                if(value.getVc() > threshold) {
                    out.collect("水位线超出阈值：" + (value.getVc() - threshold));
                }
            }

            @Override
            public void processBroadcastElement(Integer value, BroadcastProcessFunction<WaterSensor, Integer, String>.Context ctx, Collector<String> out) throws Exception {
                BroadcastState<String, Integer> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                broadcastState.put("threshold", value);
            }
        }).print();
        env.execute();
    }
}
