package com.study.checkpoint;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author Hliang
 * @create 2023-08-22 18:28
 */
public class SavepointDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        /**
         * TODO 检查点算法的总结
         *      1、Barrier对齐：一个Task收到 所有上游 同一个编号的Barrier之后，才会对本地状态 进行备份
         *          精准一次：在Barrier对齐过程中，Barrier后面的数据 阻塞等待（不会越过Barrier）
         *          至少一次：在Barrier对齐过程中，Barrier后面的数据不会阻塞等待，会继续被Task进行处理计算，故障恢复后可能会出现重复数据的情况
         *      2、Barrier非对齐：一个Taks收到 第一个 Barrier时，就开始备份，能保证 精准一次（flink 1.11出的新算法）
         *          先到的Barrier，将 本地状态 备份，其后面的数据接着计算输出
         *          后到的Barrier，其 前面的数据 接着计算输出，同时也保存到 备份中
         *          最后一个Barrier到达 该Task时，这个Task的备份结束
         */

        // TODO 开启ChangeLog 状态后端
        //  要求checkpoint的最大并发必须为1，其它参数j建议在flink-conf配置文件中配置
        env.enableChangelogStateBackend(true);


        // TODO 开启检查点，并进行相关配置
        // 开启检查点，间隔时间5s一次执行一次检查点保存，检查点模式为精确一次  (默认为Barrier对齐方式，精准一次)
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);


        // TODO 配置hdfs的访问用户名
        System.setProperty("HADOOP_USER_NAME","liangheee");

        // 进行检查点的相关配置
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        // 设置检查点保存到外部持久化存储的位置
        // 如果使用的是hdfs，那么必须引入hadoop-client的相关依赖，并启动hdfs，还要指定hdfs的用户名，解决一些权限问题
        checkpointConfig.setCheckpointStorage(new Path("hdfs://hadoop102:8020/chk"));
        // 设置checkpoint的超时时间为1分钟，默认为10分钟
        checkpointConfig.setCheckpointTimeout(60000);
        // 设置checkpoint的最大并发，最多同时进行的checkpoint的个数为2，默认为1
//        checkpointConfig.setMaxConcurrentCheckpoints(2);
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        // 设置checkpoint的间隔时间，多个checkpoint的保存间隔时间为1s，默认为0s
        checkpointConfig.setMinPauseBetweenCheckpoints(1000);
        // 作业取消时，checkpoint在外部持久化存储系统上的存储文件是否被删除，这个配置必须自己手动指定
        // DELETE_ON_CANCELLATION：job被取消时，hdfs上相关checkpoint的数据会被删除
        // RETAIN_ON_CANCELLATION；job被取消时，hdfs上的相关checkpoint的数据不会被删除
        // TODO 注意：如果flink程序被异常终止，那么该策略无论配置什么都不会产生实际作用，外部系统上的checkpoint相关文件都会保存下来
//        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 设置checkpoint允许的连续失败的次数，默认为0===》0表示一旦checkpoint失败，job就挂掉
        checkpointConfig.setTolerableCheckpointFailureNumber(10);

        // 开启非对齐方式（checkpoint默认为对齐）
        // 要求：checkpoint模式必须为精准一次，最大并发设置为1
        checkpointConfig.enableUnalignedCheckpoints();
        // 开启非对齐检查点才生效：默认为0，表示一开始就直接用 非对齐检查点
        // 如果大于0，一开始用 对齐的检查点（Barrier对齐），对齐的时间超过了设置的超时时间，自动切换成 非对齐检查点（Barrier非对齐）
        checkpointConfig.setAlignedCheckpointTimeout(Duration.ofSeconds(1));



        env.socketTextStream("hadoop102",7777)
                .flatMap(
                        (String value, Collector<Tuple2<String,Integer>> out) -> {
                            String[] words = value.split(" ");
                            for (String word : words) {
                                out.collect(Tuple2.of(word,1));
                            }
                        }
                ).uid("flatmap-wc").name("wc-flatmap")
                .returns(Types.TUPLE(Types.STRING,Types.INT))
                .keyBy(value -> value.f0)
                .sum(1).uid("sum-wc")
                .print().uid("print-wc");

        env.execute();
    }
}
