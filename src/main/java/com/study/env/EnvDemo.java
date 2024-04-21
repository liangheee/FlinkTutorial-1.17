package com.study.env;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author Hliang
 * @create 2023-08-12 11:56
 */
public class EnvDemo {
    public static void main(String[] args) throws Exception {
        /*
        getExecutionEnvironment底层实现：
       获取当前上下文环境对象工厂
                默认是本地运行环境；如果通过命令行调用，则是集群运行环境
                standalone运行模式下调用getExecutionEnvironment也是创建本地运行环境
         */
        // 我们可以自定义configuration，做一些自己的配置
        Configuration configuration = new Configuration();
        configuration.set(RestOptions.BIND_PORT,"8082");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);


        // flink1.17是流批一体的代码
        // 无论是流处理还是批处理，我们都共用同一套API
        // 默认是STREAMING处理
        // 一般不在代码里面写死，提交时，通过参数指定：-Dexecution.runtime-mdoe=BATCH
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);


        env.readTextFile("input/word.txt")
                .flatMap((String line, Collector< Tuple2<String,Integer> > collector) -> {
                    String[] words = line.split(" ");
                    for (String word : words) {
                        Tuple2<String, Integer> tuple2 = new Tuple2<>(word, 1);
                        collector.collect(tuple2);
                    }
                }).returns(Types.TUPLE(Types.STRING,Types.INT))
                .keyBy(value -> value.f0)
                .sum(1)
                .print();

//        DataStreamSource<String> source = env.socketTextStream("hadoop102", 7777);
//        source.print();

        // 同步执行，运行到这里就会阻塞，后面代码不会执行
        // 一个execute开启一个flink job
        env.execute();

        /** TODO 关于execute总结(了解)
         *     1、默认 env.execute()触发一个flink job：
         *          一个main方法可以调用多个execute，但是没意义，指定到第一个就会阻塞住
         *     2、env.executeAsync()，异步触发，不阻塞
         *         => 一个main方法里 executeAsync()个数 = 生成的flink job数
         *     3、思考：
         *         yarn-application 集群，提交一次，集群里会有几个flink job？
         *         =》 取决于 调用了n个 executeAsync()
         *         =》 对应 application集群里，会有n个job
         *         =》 对应 Jobmanager当中，会有 n个 JobMaster
         */

        // 异步执行，运行到这里不会阻塞，后面代码仍然会执行
        // 一个executeAsync开启一个flink job
//        env.executeAsync();
//        env.executeAsync();
    }
}
