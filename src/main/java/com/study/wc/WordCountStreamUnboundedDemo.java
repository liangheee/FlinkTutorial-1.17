package com.study.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * TODO 流处理 （端口数据-无界流）
 * TODO 特点：启动执行后，运行窗口不会结束，会一直监听无界流，除非手动关闭运行窗口
 * @author Hliang
 * @create 2023-08-10 10:23
 */
public class WordCountStreamUnboundedDemo {
    public static void main(String[] args) throws Exception {
        // TODO 1.获取运行环境
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();


        // TODO 2.监听端口数据
        DataStreamSource<String> dataStreamSource = executionEnvironment.socketTextStream("localhost", 7777);

        // TODO 3.切分、转换、分组、聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumDS = dataStreamSource.flatMap(
                (String line, Collector<Tuple2<String, Integer>> collector) -> {
                    String[] words = line.split(" ");
                    for (String word : words) {
                        Tuple2<String, Integer> tuple2 = new Tuple2<>(word, 1);
                        collector.collect(tuple2);
                    }
                }
        ).returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(value -> value.f0)
                .sum(1);

        // TODO 4.打印输出结果
        sumDS.print();

        // TODO 5.开启执行
        executionEnvironment.execute();

    }
}
