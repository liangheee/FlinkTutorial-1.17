package com.study.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * TODO 批处理（文件-有界流）
 * TODO 行动算子执行后，读取完有界流，运行窗口结束
 * @author Hliang
 * @create 2023-08-10 9:59
 */
public class WordCountBatchDemo {
    public static void main(String[] args) throws Exception {
        // TODO 1. 创建运行环境
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();

        // TODO 2.从文件中读取数据
        DataSource<String> dataSource = executionEnvironment.readTextFile("input/word.txt");

        // TODO 3.切分、扁平化、转换
        FlatMapOperator<String, Tuple2<String, Integer>> flatMapDS = dataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                // TODO 3.1 按照空格切分短语
                String[] words = s.split(" ");
                // TODO 3.2 使用采集器向下游采集数据
                for (String word : words) {
                    Tuple2<String, Integer> tuple2 = new Tuple2<>(word, 1);
                    collector.collect(tuple2);
                }
            }
        });

        // TODO 4.分组 按照（word,1）中索引位置为0的位置进行分组
        UnsortedGrouping<Tuple2<String, Integer>> groupByDS = flatMapDS.groupBy(0);

        // TODO 5.聚合 按照(word,1)中的索引位置为1的位置进行相加聚合
        AggregateOperator<Tuple2<String, Integer>> aggregateDS = groupByDS.sum(1);

        // TODO 6.打印结果
        aggregateDS.print();
    }
}
