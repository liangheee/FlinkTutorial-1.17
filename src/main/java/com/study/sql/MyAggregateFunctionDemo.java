package com.study.sql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author Hliang
 * @create 2023-09-06 0:46
 */
public class MyAggregateFunctionDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStreamSource<Tuple3<String, Integer, Integer>> scoreDS = env.fromElements(
                Tuple3.of("zs", 80, 2),
                Tuple3.of("zs", 95, 4),
                Tuple3.of("zs", 100, 4),
                Tuple3.of("ls", 78, 2),
                Tuple3.of("ls", 88, 4),
                Tuple3.of("ls", 98, 4)
        );

        Table table = tableEnv.fromDataStream(scoreDS,
                $("f0").as("name"),$("f1").as("subject"),$("f2").as("weight"));
        tableEnv.createTemporaryView("score",table);

        tableEnv.createTemporarySystemFunction("WeightAvg",WeightAvg.class);

        tableEnv.sqlQuery("select name,WeightAvg(subject,weight) from score group by name").execute().print();


    }


    // TODO 1、自定义类继承AggregateFunction，定义泛型
    //      第一个泛型含义：函数输出的数据类型
    //      第二个泛型含义：累加器类型  <score,weight>  <分数，权重>
    public static class WeightAvg extends AggregateFunction<Double, Tuple2<Integer,Integer>> {

        @Override
        public Double getValue(Tuple2<Integer, Integer> integerIntegerTuple2) {
            return integerIntegerTuple2.f0 * 1D / integerIntegerTuple2.f1;
        }

        @Override
        public Tuple2<Integer, Integer> createAccumulator() {
            return Tuple2.of(0,0);
        }

        /**
         * 累加计算方法，每来一行数据就会调用一次
         * @param acc 累加器类型
         * @param score 第一个参数：分数
         * @param weight 第二个参数：权重
         */
        public void accumulate(Tuple2<Integer,Integer> acc,Integer score,Integer weight){
            acc.f0 += score * weight; // 加权总和 =  分数1 * 权重1 + 分数2 * 权重2 +....
            acc.f1 += weight; // 权重和 = 权重1 + 权重2 +....
        }
    }
}
