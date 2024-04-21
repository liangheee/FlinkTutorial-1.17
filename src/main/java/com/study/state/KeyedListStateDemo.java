package com.study.state;

import com.study.bean.WaterSensor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * @author Hliang
 * @create 2023-08-19 19:07
 */
public class KeyedListStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("hadoop102", 7777)
                .map(value -> {
                    String[] split = value.split(",");
                    return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                });

        // TODO 针对每种传感器输出最高的3个水位值
        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(value -> value.getId());

        sensorKS.process(
                new KeyedProcessFunction<String, WaterSensor, String>() {

                    // 创建一个列表状态
                    ListState<Integer> maxVcListState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);

                        // 初始化列表状态
                        maxVcListState = getRuntimeContext().getListState(new ListStateDescriptor<Integer>("maxVcState",Types.INT));
                    }

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        // 获取水位线的值
                        Integer vc = value.getVc();
                        // 取出列表状态中的所有的水位线，加入到List集合中
                        Iterable<Integer> allVc = maxVcListState.get();
                        List<Integer> vcList = new ArrayList<>();
                        for (Integer temp : allVc) {
                            vcList.add(temp);
                        }
                        // 添加当前的水位线到List集合中
                        vcList.add(vc);
                        // 对List集合进行从大到小排序，也就是对水位线进行从大到小排序
                        vcList.sort(new Comparator<Integer>() {
                            @Override
                            public int compare(Integer o1, Integer o2) {
                                return o2 - o1;
                            }
                        });

                        // 如果List集合中的元素个数超过3个，则移除第3个
                        // 所以说一旦到第4个元素，那我们就清除它（它的索引为3）
                        if(vcList.size() > 3){
                            vcList.remove(3);
                        }

                        // 更新列表状态
                        maxVcListState.update(vcList);

                        // 输出当前的前3名水位线值
                        out.collect("传感器id为" + value.getId() + ",最大的3个水位值=" + vcList.toString());

                        // TODO 列表状态相关的操作总结如下
    //                        maxVcListState.get(); // 取出本分组 List状态 中的所有数据，是一个Iterable
    //                        maxVcListState.add(); // 向本分组 List状态 中添加数据
    //                        maxVcListState.addAll(); // 向本分组 List状态 中批量添加数据
    //                        maxVcListState.update(); // 更新本分组 List状态 中的所有数据
    //                        maxVcListState.clear(); // 删除本分组 List状态

                    }
                }
        ).print();


        env.execute();

    }
}
