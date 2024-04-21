package com.study.watermark;

import com.study.partition.MyPartitioner;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author Hliang
 * @create 2023-08-17 19:45
 */
public class WatermarkIdlenessDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 通过指定分区器，让数据根据奇数和偶数的区别分别进入相应的分区（子任务）
        // 奇数一个分区，偶数一个分区
        SingleOutputStreamOperator<Integer> socketDS = env.socketTextStream("hadoop102", 7777)
                .partitionCustom(new MyPartitioner(),value -> value)
                .map(value -> Integer.parseInt(value));

        SingleOutputStreamOperator<Integer> socketDSWithWatermark = socketDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Integer>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Integer>() {
                            @Override
                            public long extractTimestamp(Integer element, long recordTimestamp) {
                                return element * 1000L;
                            }
                        })
                        // TODO 指定空闲等待时间5s，如果上游某个子任务一直没有数据到来，持续5s，则不再等待其watermark，
                        //  也就是说其watermark不参与上游所有子任务的最小值比较
                        .withIdleness(Duration.ofSeconds(5))
        );

        KeyedStream<Integer, Integer> socketKS = socketDSWithWatermark.keyBy(value -> value % 2);

        /**
         * TODO 运行结果解析
         *  process算子也有会有两个并行度
         *  我们进行了keyBy操作，所以相同key的数据会进入到同一个分区，也就是奇数进入一个分区，偶数进入一个分区
         *  由于测试中我们只输入奇数，所以偶数分区是没有值的
         *  但是根据多并行度下watermark的传递规则：每个算子的并行度取决于上游到达该算子的最小watermark值
         *
         *  分析：上游奇数分区watermark是多少就是多少，上游偶数分区由于没有数据，所以watermark就是最小值Long.minValue
         *  因此，如果我们不指定空闲等待时间，下游process算子就一直不会被触发，因为process算子的watermark值一直是Long.minValue
         */
        socketKS.window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<Integer, String, Integer, TimeWindow>() {
                    @Override
                    public void process(Integer integer, Context context, Iterable<Integer> elements, Collector<String> out) throws Exception {
                        long startTs = context.window().getStart();
                        long endTs = context.window().getEnd();
                        String windowStart = DateFormatUtils.format(startTs, "yyyy-MM-dd HH:mm:ss.SSS");
                        String windowEnd = DateFormatUtils.format(endTs, "yyyy-MM-dd HH:mm:ss.SSS");

                        long count = elements.spliterator().estimateSize();

                        out.collect("key=" + integer + "的窗口[" + windowStart + "," + windowEnd + ")包含" + count + "条数据===>" + elements.toString());

                    }
                }).print();


        env.execute();
    }
}
