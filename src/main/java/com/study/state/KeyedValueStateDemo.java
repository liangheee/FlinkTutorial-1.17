package com.study.state;

import com.study.bean.WaterSensor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author Hliang
 * @create 2023-08-19 18:06
 */
public class KeyedValueStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("hadoop102", 7777)
                .map(value -> {
                    String[] split = value.split(",");
                    return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                });

        // TODO 检测每种传感器的水位值，如果连续的两个水位值超过10，就输出报警
        sensorDS.keyBy(sensor -> sensor.getId())
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {

                    // int lastVc = 0;  // 不能使用普通变量 可以使用hashmap，但是效率较低
                    // TODO 使用键控状态中的值状态
                    ValueState<Integer> lastVcSate;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        // TODO 对值状态进行初始化
                        //      初始化必须在算子的生命周期open中进行，因为根据类的加载顺序而言，在定义属性lastVcState的时候，运行环境上下文对象还没有初始化
                        /**
                         * ValueStateDescriptor值状态描述对象
                         *  第一个参数：表示值状态描述对象的名字，必须唯一
                         *  第二个参数：表示值状态对象中存储的值类型
                         */
                        lastVcSate = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("lastVcState", Types.INT));
                    }

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
//                        lastVcSate.value(); // 获取值状态中保存的值
//                        lastVcSate.update(); // 更新值状态中的值
//                        lastVcSate.clear(); // 清除该值状态

                        // 1、获取当前传感器的上一个水位线
                        Integer lastVc = lastVcSate.value();

                        // 2、与当前水位线比较，超过10则报警
                        Integer currVc = value.getVc();
                        if(Math.abs(lastVc - currVc) > 10){
                            out.collect("传感器" + ctx.getCurrentKey() + "中，当前水位线=" + currVc + "，上一条水位线=" + lastVc + "，相差超过10！！！");
                        }

                        // 3、更新值状态中的水位线值
                        lastVcSate.update(currVc);
                    }
                }).print();


        env.execute();
    }
}
