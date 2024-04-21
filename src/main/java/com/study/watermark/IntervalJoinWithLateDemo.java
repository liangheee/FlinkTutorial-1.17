package com.study.watermark;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @author Hliang
 * @create 2023-08-18 11:55
 */
public class IntervalJoinWithLateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple2<String, Integer>> socketDSWithWatermark1 = env.socketTextStream("hadoop102", 7777)
                .map(value -> {
                    String[] split = value.split(",");
                    return new Tuple2<String, Integer>(split[0], Integer.parseInt(split[1]));
                }).returns(Types.TUPLE(Types.STRING,Types.INT))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((t, recordTimestamp) -> t.f1 * 1000)
                );

        SingleOutputStreamOperator<Tuple3<String, Integer,Integer>> socketDSWithWatermark2 = env.socketTextStream("hadoop102", 8888)
                .map(value -> {
                    String[] split = value.split(",");
                    return new Tuple3<String, Integer,Integer>(split[0], Integer.parseInt(split[1]),Integer.parseInt(split[2]));
                }).returns(Types.TUPLE(Types.STRING,Types.INT,Types.INT))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple3<String, Integer,Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((t, recordTimestamp) -> t.f1 * 1000)
                );

        KeyedStream<Tuple2<String, Integer>, String> socketKSWithWatermark1 = socketDSWithWatermark1.keyBy(t -> t.f0);
        KeyedStream<Tuple3<String, Integer, Integer>, String> socketKSWithWatermark2 = socketDSWithWatermark2.keyBy(t -> t.f0);

        OutputTag<Tuple2<String, Integer>> leftOutputTag = new OutputTag<>("left-late", Types.TUPLE(Types.STRING, Types.INT));
        OutputTag<Tuple3<String, Integer, Integer>> rightOutputTag = new OutputTag<>("right-late", Types.TUPLE(Types.STRING, Types.INT, Types.INT));

        /**
         * TODO 总结
         *      1、只支持事件时间
         *      2、通过between指定上界、下界的偏移，负号代表时间向前，正号代表时间向后
         *      3、process中，只能处理join的数据
         *      4、间隔联结中，水位线还是和多并行度的水位线传递机制一样，两条流关联后的watermark，以两条流中最小的为准
         *      5、如果 当前数据的事件时间 < 当前的watermark，就是迟到数据，主流的process不进行处理
         *          =》between后，可以指定 左流 或 右流 的迟到数据 放入侧输出流
         */
        SingleOutputStreamOperator<String> process = socketKSWithWatermark1.intervalJoin(socketKSWithWatermark2)
                .between(Time.seconds(-2), Time.seconds(2))
                // TODO 左流的侧输出流  （intervalJoin算子左边的流，也就是调用intervalJoin算子的流）
                .sideOutputLeftLateData(leftOutputTag)
                // TODO 右流的侧输出流  （intervalJoin算子右边的流，也就是intervalJoin算子参数中的流）
                .sideOutputRightLateData(rightOutputTag)
                .process(new ProcessJoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>() {
                    @Override
                    public void processElement(Tuple2<String, Integer> left, Tuple3<String, Integer, Integer> right, Context ctx, Collector<String> out) throws Exception {
                        out.collect(left + " <-------> " + right);
                    }
                });

        // 输出主流
        process.print("主流");
        // 输出侧流
        process.getSideOutput(leftOutputTag).printToErr("左流迟到数据");
        process.getSideOutput(rightOutputTag).printToErr("右流迟到数据");

        env.execute();
    }
}
