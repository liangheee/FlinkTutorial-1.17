package com.study.state;

import com.study.bean.WaterSensor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

/**
 * @author Hliang
 * @create 2023-08-19 20:51
 */
public class KeyedMapStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("hadoop102", 7777)
                .map(value -> {
                    String[] split = value.split(",");
                    return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                });

        // TODO 案例需求：统计每种传感器每种水位值出现的次数
        //  分析一下：这里其实分组key可以是两个指标（传感器id+水位值）
        //          但是如果水位值较多的话，这里就会产生很多状态，默认采用HashMapStateBackend，对内存消耗较大（受taskManager的jvm内存限制）
        //          所以我们，这里仅仅对传感器id作为指标进行keyBy，然后内部状态采用MapState，其中key为水位值，而value为次数
        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(value -> value.getId());

        sensorKS.process(new KeyedProcessFunction<String, WaterSensor, String>() {

            MapState<Integer,Integer> vcCountMapState;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                vcCountMapState = getRuntimeContext().getMapState(new MapStateDescriptor<Integer, Integer>("vcCountMapState", Types.INT,Types.INT));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                // 获取vc值
                Integer vc = value.getVc();
                // 判断Map状态中是否含有该vc值
                if(vcCountMapState.contains(vc)){
                    // MapState中含有该vc，则更新vcCountMapState中vc对应的count值
                    Integer count = vcCountMapState.get(vc);
                    vcCountMapState.put(vc,++count);
                }else{
                    // MapState中没有该vc，则初始化vcCountMapState
                    vcCountMapState.put(vc,1);
                }

                // 输出
                StringBuilder outStr = new StringBuilder();
                outStr.append("======================================\n");
                outStr.append("传感器id为" + value.getId() + "\n");
                for (Map.Entry<Integer, Integer> vcCount : vcCountMapState.entries()) {
                    outStr.append(vcCount.toString() + "\n");
                }
                outStr.append("======================================\n");

                out.collect(outStr.toString());

                // TODO MapSate的常用方法总结
    //                vcCountMapState.get(); // 获取本分组 的Map状态里 对应的key的值
    //                vcCountMapState.entries(); // 获取本分组的 Map状态里 所有的entry，返回的是一个 Iterable<Map.Entry<KeyType,valueType>>
    //                vcCountMapState.keys(); // 获取本分组的 Map状态里 所有的key
    //                vcCountMapState.values(); // 获取本分组的 Map状态里 所有的value
    //                vcCountMapState.contains(); // 判断本分组的 Map状态里 是否包含指定key
    //                vcCountMapState.isEmpty(); // 判断本分组的 Map状态是否为空
    //                vcCountMapState.put(); // 向本分组的 Map状态中添加数据
    //                vcCountMapState.putAll(); // 向本分组的Map状态中批量添加数据
    //                vcCountMapState.remove(); // 删除本分组Map状态中的 指定 key
    //                vcCountMapState.iterator(); // 获取本分组 Map状态中的 所有的entry，返回的是一个 Iterator<Map.Entry<KeyType,valueType>>
    //                vcCountMapState.clear(); // 清除 本分组 Map状态

            }
        }).print();

        env.execute();
    }
}
