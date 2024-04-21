package com.study.partition;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Hliang
 * @create 2023-08-14 10:53
 */
public class PartitionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 7777);

        // shuffle随机分区： 采用的ShufflePartitioner，里面的selectChannel方法return random.nextInt(numberOfChannels);
        // 其中numberOfChannels是下游算子的并行度
//        DataStream<String> result = socketTextStream.shuffle();

        // rebalance轮询分区：第一个分区随机选择后，进行轮询发牌
        // 里面的selectChannel方法
        // nextChannelToSendTo = (nextChannelToSendTo + 1) % numberOfChannels;
        // return nextChannelToSendTo;
        // TODO 如果是数据倾斜的场景，source后，调用rebalance，就可以解决数据源的数据倾斜问题
//        DataStream<String> result = socketTextStream.rebalance();

        // rescale：缩放
        // 缩放底层也是使用的Round-Robin算法进行轮询的，但是只会将数据发往下游并行任务中的一部分中
        // 局部组队，效率比rebalance更高
//        DataStream<String> result = socketTextStream.rescale();

        // broadcast：广播  发送给下游所有的子任务
        // 所以不需要走selectChannel方法，如果强制调用selectChannel方法则会抛出异常
//        DataStream<String> result = socketTextStream.broadcast();

        // global：全局：全部发往第一个子任务中
        // 相当于强制让下游子任务的并行度变成了1，一定要慎重使用
        // public int selectChannel(SerializationDelegate<StreamRecord<T>> record) {
        //        return 0;
        // }
        DataStream<String> result = socketTextStream.global();

        result.print();

        env.execute();
    }
}
