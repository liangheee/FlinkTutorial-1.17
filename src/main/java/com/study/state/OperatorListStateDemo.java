package com.study.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Hliang
 * @create 2023-08-19 22:45
 */
public class OperatorListStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // TODO 在map算子中计算数据的个数
        env.socketTextStream("hadoop102", 7777)
                .map(new MyCountMapFunction())
                .print();


        env.execute();
    }

    // TODO 1、实现 CheckpointedFunction接口
    public static class MyCountMapFunction implements MapFunction<String,Long>, CheckpointedFunction {
        private Long count = 0L;
        private ListState<Long> state;

        /**
         * TODO 2、本地变量持久化：将 本地变量 拷贝到 算子状态中，开启checkpoint时才会调用
         *  故障后，本地变量可以通过这个的备份进行恢复
         * @param context
         * @throws Exception
         */
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            System.out.println("snapshotState......");
            // 2.1 清空算子状态
            state.clear();
            // 2.2 将本地变量 添加到 算子状态中
            state.add(count);
        }

        /**
         * TODO 3、初始化变量：程序启动和恢复时，从状态中 把数据添加到 本地变量，每个子任务调用一次
         * @param context
         * @throws Exception
         */
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            System.out.println("initializeState.....");

            /**TODO
             *  算子状态中， list 与 unionlist的区别：  并行度改变时，怎么重新分配状态
             *  1、list状态：  轮询均分 给 新的 并行子任务
             *  2、unionlist状态： 原先的多个子任务的状态，将状态中的数据合并成一份完整的。 会把 完整的列表 广播给 新的并行子任务 （每人一份完整的）
             */

            // 3.1 从上下文 初始化算子状态
            state = context.getOperatorStateStore()
//                    .getListState(new ListStateDescriptor<Long>("state", Types.LONG));
                    .getUnionListState(new ListStateDescriptor<Long>("state", Types.LONG));

            // 3.2 从算子状态中 把数据拷贝到 本地变量
            if(context.isRestored()){
                // 如果上下文恢复好
                for(Long c: state.get()){
                    count += c;
                }
            }
        }

        /**
         * 进行Map操作
         * @param value
         * @return
         * @throws Exception
         */
        @Override
        public Long map(String value) throws Exception {
            return ++count;
        }
    }
}
