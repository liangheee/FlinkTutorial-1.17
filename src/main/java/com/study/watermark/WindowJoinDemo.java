package com.study.watermark;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author Hliang
 * @create 2023-08-18 11:31
 */
public class WindowJoinDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        SingleOutputStreamOperator<Tuple2<String, Integer>> source1WithWatermark = env.fromElements(
                new Tuple2<String, Integer>("a", 1),
                new Tuple2<String, Integer>("a", 2),
                new Tuple2<String, Integer>("b", 3),
                new Tuple2<String, Integer>("c", 4)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple2<String, Integer>>forMonotonousTimestamps()
                        .withTimestampAssigner((t, recordTimestamp) -> t.f1 * 1000)
        );

        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> source2WithWatermark = env.fromElements(
                new Tuple3<String, Integer, Integer>("a", 4, 4),
                new Tuple3<String, Integer, Integer>("a", 11, 11),
                new Tuple3<String, Integer, Integer>("b", 4, 4),
//                new Tuple3<String, Integer, Integer>("b", 10, 4), 这两条数据主要用户测试cogroup在join时的延迟等待事件
//                new Tuple3<String, Integer, Integer>("b", 9, 4),
                new Tuple3<String, Integer, Integer>("c", 6, 6),
                new Tuple3<String, Integer, Integer>("c", 12, 12),
                new Tuple3<String, Integer, Integer>("d", 13, 13)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<String, Integer, Integer>>forMonotonousTimestamps()
                        .withTimestampAssigner((t, recordTimestamp) -> t.f1 * 1000)
        );

        /**
         * TODO Window Join的说明
         *      1、落在同一个时间窗口范围内才能匹配
         *      2、根据keyBy的key，来进行匹配关联
         *      3、只能拿上匹配的数据，类似有固定时间范围的inner join
         *  注意：多个并行度下，在进行join之前需要进行keyBy，否则就可能漏掉匹配数据
         */
        DataStream<String> joinDS = source1WithWatermark.join(source2WithWatermark)
                .where(s1 -> s1.f0, Types.STRING)
                .equalTo(s2 -> s2.f0, Types.STRING)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(new JoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>() {
                    /**
                     * key匹配上的元素的join方法
                     * @param first source1WithWatermark的数据
                     * @param second source2WithWatermark的数据
                     * @return
                     * @throws Exception
                     */
                    @Override
                    public String join(Tuple2<String, Integer> first, Tuple3<String, Integer, Integer> second) throws Exception {
                        return first + "<----------->" + second;
                    }
                });

        // 使用CoGroup实现的左外连接
        DataStream<String> leftJoinDS = source1WithWatermark.coGroup(source2WithWatermark)
                .where(s1 -> s1.f0, Types.STRING)
                .equalTo(s2 -> s2.f0, Types.STRING)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.seconds(10))
                .apply(new LeftJoinFunction());


//        joinDS.print();
        leftJoinDS.print();

        env.execute();
    }
}
class InnerJoinFunction implements CoGroupFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String> {

    @Override
    public void coGroup(Iterable<Tuple2<String, Integer>> first, Iterable<Tuple3<String, Integer, Integer>> second, Collector<String> out) throws Exception {
        for(Tuple2<String,Integer> left : first){
            for(Tuple3<String,Integer,Integer> right : second){
                if(left.f0.equals(right.f0)){
                    out.collect(first + "<----------->" + second);
                }
            }
        }
    }
}

class LeftJoinFunction implements CoGroupFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String> {

    @Override
    public void coGroup(Iterable<Tuple2<String, Integer>> first, Iterable<Tuple3<String, Integer, Integer>> second, Collector<String> out) throws Exception {
        for(Tuple2<String,Integer> left : first){
            boolean onEmit = false;
            for(Tuple3<String,Integer,Integer> right : second){
                if(left.f0.equals(right.f0)){
                    onEmit = true;
                    out.collect(left + "<----------->" + right);
                }
            }
            if(!onEmit){
                out.collect(left + "<----------->");
            }
        }
    }
}

class RightJoinFunction implements CoGroupFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String> {

    @Override
    public void coGroup(Iterable<Tuple2<String, Integer>> first, Iterable<Tuple3<String, Integer, Integer>> second, Collector<String> out) throws Exception {
        for(Tuple3<String,Integer,Integer> right : second){
            boolean onEmit = false;
            for(Tuple2<String,Integer> left : first){
                if(right.f0.equals(left.f0)){
                    onEmit = true;
                    out.collect(left + "<----------->" + right);
                }
            }
            if(!onEmit){
                out.collect("<----------->" + right);
            }
        }
    }
}
