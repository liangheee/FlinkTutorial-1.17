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

/**
 * @author Hliang
 * @create 2023-08-20 0:09
 */
public class OperatorBroadcastStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        // TODO 水位超过指定的阈值发送告警，阈值可以动态修改。

        // TODO 1、创建数据流
        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("hadoop102", 7777)
                .map(value -> {
                    String[] split = value.split(",");
                    return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                });

        // TODO 2、创建配置流（广播流），动态配置指定阈值  （配置一般就是Map键值对）
        SingleOutputStreamOperator<Integer> configDS = env.socketTextStream("hadoop102", 8888)
                .map(value -> Integer.valueOf(value));

        // TODO 3、将配置流广播
        MapStateDescriptor<String, Integer> mapStateDescriptor = new MapStateDescriptor<>("boardcast-state", Types.STRING, Types.INT);
        BroadcastStream<Integer> configBS = configDS.broadcast(mapStateDescriptor);

        // TODO 4、将数据流和配置流联接
        BroadcastConnectedStream<WaterSensor, Integer> sensorBCS = sensorDS.connect(configBS);

        sensorBCS.process(new BroadcastProcessFunction<WaterSensor, Integer, String>() {
            @Override
            public void processElement(WaterSensor value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                // TODO 6、通过上下文获取广播状态，取出里面的值（只读，不能修改）
                ReadOnlyBroadcastState<String, Integer> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                Integer threshold = broadcastState.get("threshold");
                // TODO ！！！判断广播状态里是否有数据，因为刚启动时，可能是数据流的第一条数据先来
                threshold = threshold == null ? 0 : threshold; // Integer的值空值为null
                if(value.getVc() > threshold){
                    out.collect(value + ",水位超过指定的阈值：" + threshold + "!!!");
                }
            }

            /**
             * 广播后的配置流的处理方法：只有广播流（配置流）才能进行修改 广播状态
             * @param value
             * @param ctx
             * @param out
             * @throws Exception
             */
            @Override
            public void processBroadcastElement(Integer value, Context ctx, Collector<String> out) throws Exception {
                // TODO 5、通过上下文获取广播状态，往里面写数据
                BroadcastState<String, Integer> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                broadcastState.put("threshold",value);
            }
        }).print();


        env.execute();
    }
}
