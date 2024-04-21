package com.study.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * TODO 集合当作数据源
 * @author Hliang
 * @create 2023-08-12 11:13
 */
public class CollectionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从集合中读
//        DataStreamSource<Integer> source = env.fromCollection(Arrays.asList(1, 2, 3, 45));

        // 从元素中读
        DataStreamSource<Integer> source = env.fromElements(1, 2, 3, 4);

        source.print();

        env.execute();
    }
}
