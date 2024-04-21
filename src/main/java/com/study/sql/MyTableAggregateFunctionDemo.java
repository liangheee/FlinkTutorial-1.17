package com.study.sql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @author Hliang
 * @create 2023-09-06 1:00
 */
public class MyTableAggregateFunctionDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        //  姓名，分数，权重
        DataStreamSource<Integer> numDS = env.fromElements(3, 6, 12, 5, 8, 9, 4);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Table numTable = tableEnv.fromDataStream(numDS, $("num"));

        // TODO 2.注册函数
        tableEnv.createTemporaryFunction("Top2", Top2.class);

        // TODO 3.调用 自定义函数: 只能用 Table API
        numTable
                .flatAggregate(call("Top2", $("num")).as("value", "rank"))
                .select( $("value"), $("rank"))
                .execute().print();
    }

    // TODO 1、自定义类继承TableAggregateFunction，说明泛型
    //      第一个泛型：输出数据类型 <12,1> <9,2> <值，排名>
    //      第二个泛型：累加器类型 <第一大的值，第二大的值>
    public static class Top2 extends TableAggregateFunction<Tuple2<Integer,Integer>,Tuple2<Integer,Integer>> {

        @Override
        public Tuple2<Integer, Integer> createAccumulator() {
            return Tuple2.of(Integer.MIN_VALUE,Integer.MIN_VALUE);
        }

        /**
         * 每一来一行，就调用一次该方法，计算一次结果
         * @param acc 累加器
         * @param num 每一行传入的值
         */
        public void accumulate(Tuple2<Integer,Integer> acc,Integer num){
            if(acc.f0 < num){
                acc.f1 = acc.f0;
                acc.f0 = num;
            }else if(acc.f1 < num){
                acc.f1 = num;
            }
        }

        /**
         * 发射输出
         * @param acc 累加器
         * @param out 输出采集器
         */
        public void emitValue(Tuple2<Integer,Integer> acc, Collector<Tuple2<Integer, Integer>> out){
            if(acc.f0 != Integer.MIN_VALUE){
                out.collect(Tuple2.of(acc.f0,1));
            }

            if(acc.f1 != Integer.MIN_VALUE){
                out.collect(Tuple2.of(acc.f1,2));
            }
        }


    }
}
