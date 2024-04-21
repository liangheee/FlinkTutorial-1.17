package com.study.window;

import com.study.bean.WaterSensor;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author Hliang
 * @create 2023-08-15 18:30
 */
public class WindowAggregateAndProcessDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 7777);
        SingleOutputStreamOperator<WaterSensor> sensorDS = socketTextStream.map(value -> {
            String[] split = value.split(",");
            return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
        });

        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(value -> value.getId());

        // TODO 以时间滚动窗口为例
        WindowedStream<WaterSensor, String, TimeWindow> sensorWS = sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        /**
         * TODO 增量聚合aggregate/reduce + 全窗口函数process
         *      1、增量聚合函数处理数据的规则：来一条处理一条
         *      2、窗口触发时，增量聚合的结果（只有一条）传递给全窗口函数
         *      3、经过全窗口函数的处理包装后输出
         *
         * TODO 下面的操作结合了增量聚合和全窗口函数的优点
         *      1、增量聚合：来一条计算一条，存储中间的计算结果，占用空间小
         *      2、全窗口函数：可以通过 上下文 实现灵活的功能
         */
        SingleOutputStreamOperator<String> aggregate = sensorWS.aggregate(new MyAgg(), new MyProcess());

        aggregate.print();

        env.execute();
    }

    public static class MyAgg implements AggregateFunction<WaterSensor,Integer,String> {

        @Override
        public Integer createAccumulator() {
            System.out.println("创建累加器，并初始化为0");
            return 0;
        }

        @Override
        public Integer add(WaterSensor value, Integer accumulator) {
            System.out.println("调用Add方法，WaterSensor=" + value + ", acuumulator=" + accumulator);
            return value.getVc() + accumulator;
        }

        @Override
        public String getResult(Integer accumulator) {
            System.out.println("调用getResult方法");
            return "累加器的结果为：" + accumulator;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return null;
        }
    }

    public static class MyProcess extends ProcessWindowFunction<String,String,String,TimeWindow>{

        @Override
        public void process(String s, Context context, Iterable<String> elements, Collector<String> out) throws Exception {
            TimeWindow window = context.window();
            String windowStart = DateFormatUtils.format(window.getStart(),"yyyy-MM-dd HH:mm:ss.SSS");
            String windowEnd = DateFormatUtils.format(window.getEnd(),"yyyy-MM-dd HH:mm:ss.SSS");

            long count = elements.spliterator().estimateSize();
            out.collect("key=" + s + "的窗口[" + windowStart + "," + windowEnd + ")包含" + count + "条数据===>" + elements.toString());
        }
    }
}
