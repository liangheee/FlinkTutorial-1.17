package com.study.combine;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Hliang
 * @create 2023-08-14 12:06
 */
public class ConnectKeyByDemo {
    public static void main(String[] args) throws Exception {
        // TODO 案例需求：连接两条流，输出能根据id匹配上的数据（类似inner join效果）
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);


        DataStreamSource<Tuple2<Integer, String>> source1 = env.fromElements(
                Tuple2.of(1, "a1"),
                Tuple2.of(1, "a2"),
                Tuple2.of(2, "b"),
                Tuple2.of(3, "c")
        );
        DataStreamSource<Tuple3<Integer, String, Integer>> source2 = env.fromElements(
                Tuple3.of(1, "aa1", 1),
                Tuple3.of(1, "aa2", 2),
                Tuple3.of(2, "bb", 1),
                Tuple3.of(3, "cc", 1)
        );


        // TODO 1.连接两个流
        ConnectedStreams<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>> connectedStreams = source1.connect(source2);

        // TODO 2.进行keyBy操作，将相同key的数据放入同一个分区（子任务）中
        ConnectedStreams<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>> keyByConnectedStreams = connectedStreams.keyBy(new KeySelector<Tuple2<Integer, String>, Integer>() {
            @Override
            public Integer getKey(Tuple2<Integer, String> value) throws Exception {
                return value.f0;
            }
        }, new KeySelector<Tuple3<Integer, String, Integer>, Integer>() {
            @Override
            public Integer getKey(Tuple3<Integer, String, Integer> value) throws Exception {
                return value.f0;
            }
        });

        // TODO 3.进行process处理，注意要存储不同流的状态
        // TODO 因为可能两个数据源的流哪个先来处理我们是不确定的，需要对每次来的流进行保存，下次来的流进行匹配再保存
        SingleOutputStreamOperator<String> process = keyByConnectedStreams.process(new CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, String>() {

            // TODO 定义保存数据状态的变量
            Map<Integer, List<Tuple2<Integer, String>>> source1Cache = new HashMap<>();
            Map<Integer, List<Tuple3<Integer, String, Integer>>> source2Cache = new HashMap<>();

            /**
             * 对第一个流中的数据进行处理
             * @param value
             * @param ctx
             * @param out
             * @throws Exception
             */
            @Override
            public void processElement1(Tuple2<Integer, String> value, Context ctx, Collector<String> out) throws Exception {
                // 判断数据value是否已经在source1Cache中存在
                Integer id = value.f0;
                if (!source1Cache.containsKey(id)) {
                    // 不存在，则添加
                    List<Tuple2<Integer, String>> source1List = new ArrayList<>();
                    source1List.add(value);
                    source1Cache.put(id, source1List);
                } else {
                    // 存在，则更新list
                    List<Tuple2<Integer, String>> list = source1Cache.get(id);
                    list.add(value);
                    source1Cache.put(id, list);
                }

                // 查找source2Cache中是否缓存有source2的数据
                // 如果缓存有，则进行join操作
                if (source2Cache.containsKey(id)) {
                    List<Tuple3<Integer, String, Integer>> list = source2Cache.get(id);
                    for (Tuple3<Integer, String, Integer> source2Elem : list) {
                        out.collect("source1:" + value + "<=============>" + "source2:" + source2Elem);
                    }
                }
            }

            /**
             * 对第二个流中的数据进行处理
             * @param value
             * @param ctx
             * @param out
             * @throws Exception
             */
            @Override
            public void processElement2(Tuple3<Integer, String, Integer> value, Context ctx, Collector<String> out) throws Exception {
                // 判断数据value是否已经在source2Cache中存在
                Integer id = value.f0;
                if (!source2Cache.containsKey(id)) {
                    // 不存在，则添加
                    List<Tuple3<Integer, String, Integer>> source2List = new ArrayList<>();
                    source2List.add(value);
                    source2Cache.put(id, source2List);
                } else {
                    // 存在，则更新list
                    List<Tuple3<Integer, String, Integer>> list = source2Cache.get(id);
                    list.add(value);
                    source2Cache.put(id, list);
                }

                // 查找source1Cache中是否缓存有source1的数据
                // 如果缓存有，则进行join操作
                if (source1Cache.containsKey(id)) {
                    List<Tuple2<Integer, String>> list = source1Cache.get(id);
                    for (Tuple2<Integer, String> source1Elem : list) {
                        out.collect("source1:" + source1Elem + "<=============>" + "source2:" + value);
                    }
                }
            }
        });

        process.print();


        env.execute();
    }
}
