package com.study.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * TODO 流处理（文件-有界流）
 * TODO 开启执行后，读取完有界流，运行窗口结束
 * @author Hliang
 * @create 2023-08-10 10:01
 */
public class WordCountStreamDemo {
    public static void main(String[] args) throws Exception {
        // TODO 1.创建运行环境
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();


        // TODO 2.读取文件
        DataStreamSource<String> dataStreamSource = executionEnvironment.readTextFile("input/word.txt");

        // TODO 3.切分、转换
        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMapDS = dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                // TODO 3.1.切分输入字符串s
                String[] words = s.split(" ");
                for (String word : words) {
                    Tuple2<String, Integer> tuple2 = new Tuple2<>(word, 1);
                    // TODO 3.2 向下游收集tuple2
                    collector.collect(tuple2);
                }
            }
        });

        // TODO 4.分组
        KeyedStream<Tuple2<String, Integer>, Object> keyByDS = flatMapDS.keyBy(new KeySelector<Tuple2<String, Integer>, Object>() {
            @Override
            public Object getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.f0;
            }
        });
        
        // TODO 5.聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumDS = keyByDS.sum(1);

        // TODO 6.打印结果
        sumDS.print();

        // TODO 7.流处理中一定要加上execute操作，类似于SparkStreaming中的ssc.start
        executionEnvironment.execute();
    }
}
