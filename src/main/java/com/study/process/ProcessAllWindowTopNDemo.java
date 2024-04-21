package com.study.process;

import com.study.bean.WaterSensor;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.*;

/**
 * @author Hliang
 * @create 2023-08-19 15:40
 */
public class ProcessAllWindowTopNDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("hadoop102", 7777)
                .map(value -> {
                    String[] split = value.split(",");
                    return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                }).assignTimestampsAndWatermarks(
                        WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((w, recordTimestamp) -> w.getTs() * 1000)
                );

        // 采用全窗口，并使用变量存储读取的到窗口数据
        SingleOutputStreamOperator<String> process = sensorDS.windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .process(new ProcessAllWindowFunction<WaterSensor, String, TimeWindow>() {

                    // 定义一个变量来存储窗口数据
                    private Map<Integer, Integer> vcCountMap = new HashMap<>();

                    @Override
                    public void process(Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        // 1、利用map来存储vc和对应的count
                        for (WaterSensor waterSensor : elements) {
                            Integer vc = waterSensor.getVc();
                            if (vcCountMap.containsKey(vc)) {
                                // 如果map中含有vc，则value值+1
                                vcCountMap.put(vc, vcCountMap.get(vc) + 1);
                            } else {
                                // 如果map中没有vc，则初始化map
                                vcCountMap.put(vc, 1);
                            }
                        }

                        // 2、对vcCountMap进行排序，输出count值前2的水位线
                        List<Tuple2<Integer, Integer>> vcCountList = new ArrayList<>();
                        for (Integer vc : vcCountMap.keySet()) {
                            vcCountList.add(Tuple2.of(vc, vcCountMap.get(vc)));
                        }

                        vcCountList.sort(new Comparator<Tuple2<Integer, Integer>>() {
                            @Override
                            public int compare(Tuple2<Integer, Integer> o1, Tuple2<Integer, Integer> o2) {
                                return o2.f1 - o1.f1;
                            }
                        });

                        // 3.取出 count最大的2个 vc
                        StringBuilder outStr = new StringBuilder();

                        outStr.append("================================\n");
                        // 遍历 排序后的 List，取出前2个， 考虑可能List不够2个的情况  ==》 List中元素的个数 和 2 取最小值
                        for (int i = 0; i < Math.min(2, vcCountList.size()); i++) {
                            Tuple2<Integer, Integer> vcCount = vcCountList.get(i);
                            outStr.append("Top" + (i + 1) + "\n");
                            outStr.append("vc=" + vcCount.f0 + "\n");
                            outStr.append("count=" + vcCount.f1 + "\n");
                            outStr.append("窗口结束时间=" + DateFormatUtils.format(context.window().getEnd(), "yyyy-MM-dd HH:mm:ss.SSS") + "\n");
                            outStr.append("================================\n");
                        }
                        out.collect(outStr.toString());
                    }
                });

        process.print();

        env.execute();
    }
}
