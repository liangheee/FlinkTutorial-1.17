package com.study.process;

import com.study.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.*;

/**
 * @author Hliang
 * @create 2023-08-19 15:55
 */
public class KeyedPorcessTimerTopNDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDSWithWatermark = env.socketTextStream("hadoop102", 7777)
                .map(value -> {
                    String[] split = value.split(",");
                    return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                }).assignTimestampsAndWatermarks(
                        WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((sensor, recordTimestamp) -> sensor.getTs() * 1000)
                );

        // 最近10秒 = 窗口长度，每5秒输出 = 滑动步长
        /**
         * TODO 思路二：使用 KeyedProcessFunction实现
         *  1、按照vc做keyBy，开窗，分别count
         *      ===》增量聚合，计算count
         *      ===》全窗口，对计算结果count值封装，带上窗口结束时间的 标签
         *            ===》 为了让同一窗口时间范围的计算结果到一起去
         *  2、对同一窗口范围的count值进行处理：排序，取前两个
         *      ===》按照windowEnd进行keyBy
         *      ===》使用process，来一条调用一次，需要先存，分开存，用HashMap，key=windowEnd，value=List
         *          =》使用定时器，对 存起来的结果 进行排序、取前两个
         */
        // 1、按照key进行分组，将相同vc的数据分到同一组
        KeyedStream<WaterSensor, Integer> sensorKSWithWatermark = sensorDSWithWatermark.keyBy(sensor -> sensor.getVc());

        // 2、对相同vc的数据进行开窗聚合操作
        // TODO ：注意一个问题：开窗之后，再调用聚合函数，会将原来的窗口信息抹掉
        SingleOutputStreamOperator<Tuple3<Integer, Integer, Long>> aggregateDS = sensorKSWithWatermark.window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new MyAgg(), new MyProcessWindowFunction());

        // 3、对聚合结果进行keyBy，按照windowEnd进行keyBy，将相同的时间窗口数据分到同一组
        KeyedStream<Tuple3<Integer, Integer, Long>, Long> aggregateKS = aggregateDS.keyBy(t -> t.f2);

        // 4、对keyBy后的结果调用process方法进行处理 ===》 排序，取前2
        SingleOutputStreamOperator<String> process = aggregateKS.process(new MyKeyedProcessFunction(2));

        process.print();

        env.execute();
    }

    public static class MyAgg implements AggregateFunction<WaterSensor,Integer,Integer>{

        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(WaterSensor value, Integer accumulator) {
            return accumulator + 1;
        }

        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return null;
        }
    }

    public static class MyProcessWindowFunction extends ProcessWindowFunction<Integer, Tuple3<Integer,Integer,Long>,Integer, TimeWindow>{

        @Override
        public void process(Integer key, Context context, Iterable<Integer> elements, Collector<Tuple3<Integer, Integer, Long>> out) throws Exception {
            // 主要是为了避免多并行度下同一窗口数据分多次输出的情况 (设置定时器，事件时间为windowEnd+10ms尽量避免该问题)
            // 原因：同一个窗口范围，应该同时输出，只不过是一条一条调用processElement方法，只需要延迟10ms即可
            long windEnd = context.window().getEnd() + 10; // 该窗口不包括该值
            Integer count = elements.iterator().next();
            out.collect(Tuple3.of(key,count,windEnd));
        }
    }

    public static class MyKeyedProcessFunction extends KeyedProcessFunction<Long,Tuple3<Integer,Integer,Long>,String> {

        private Map<Long, List<Tuple3<Integer,Integer,Long>>> vcCountMap;
        private Integer threshold;
        public MyKeyedProcessFunction(int n){
            threshold = n;
            vcCountMap = new HashMap<>();
        }

        @Override
        public void processElement(Tuple3<Integer, Integer, Long> value, Context ctx, Collector<String> out) throws Exception {
            // 来一条数据处理一条
            Long windowEnd = ctx.getCurrentKey();
            if(vcCountMap.containsKey(windowEnd)){
                // 如果存在当前key，则更新对应的值
                List<Tuple3<Integer, Integer, Long>> dataList = vcCountMap.get(windowEnd);
                dataList.add(value);
                vcCountMap.put(windowEnd,dataList);
            }else{
                // 如果不存在当前key，则初始化当前key
                List<Tuple3<Integer,Integer,Long>> dataList = new ArrayList<>();
                dataList.add(value);
                vcCountMap.put(windowEnd,dataList);
            }

            TimerService timerService = ctx.timerService();

            timerService.registerEventTimeTimer(windowEnd);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            Long windowEnd = ctx.getCurrentKey();

            List<Tuple3<Integer, Integer, Long>> dataList = vcCountMap.get(windowEnd);
            dataList.sort(new Comparator<Tuple3<Integer, Integer, Long>>() {
                @Override
                public int compare(Tuple3<Integer, Integer, Long> o1, Tuple3<Integer, Integer, Long> o2) {
                    return o2.f1 - o1.f1;
                }
            });
            StringBuilder outStr = new StringBuilder();

            outStr.append("================================\n");
            // 遍历 排序后的 List，取出前 threshold 个， 考虑可能List不够2个的情况  ==》 List中元素的个数 和 2 取最小值
            for (int i = 0; i < Math.min(threshold, dataList.size()); i++) {
                Tuple3<Integer, Integer, Long> vcCount = dataList.get(i);
                outStr.append("Top" + (i + 1) + "\n");
                outStr.append("vc=" + vcCount.f0 + "\n");
                outStr.append("count=" + vcCount.f1 + "\n");
                outStr.append("窗口结束时间=" + vcCount.f2 + "\n");
                outStr.append("================================\n");
            }

            // 用完的List，及时清理，节省资源
            dataList.clear();

            out.collect(outStr.toString());
        }
    }
}
