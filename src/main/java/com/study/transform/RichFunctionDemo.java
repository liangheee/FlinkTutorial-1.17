package com.study.transform;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Hliang
 * @create 2023-08-14 10:43
 */
public class RichFunctionDemo {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set(RestOptions.BIND_PORT,"8082");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(2);

        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop102", 7777);

        /*
            TODO RichXxxFunction：富函数
                要点：
                    1、富函数中存在有对生命周期操作的默认方法open和close
                        1）open方法：每个子任务在启动时，执行一次
                        2）close方法：每个子任务在正常结束或取消cancel时，执行一次  (IDEA强制取消或者其它异常取消不会执行close)
                    2、富函数中能够获取运行时的环境信息，比如子任务编号、名称等等（里面封装有getRuntimeContext()方法）
                    2、对于每个转换算子，都有相对应的富函数，命名为RichXxxFunction
                    3、如果我们要在每个子任务执行开始和结束进行某些必要的操作，我们可以选择使用算子对应的富函数

         */
        SingleOutputStreamOperator<Integer> map = dataStreamSource.map(new RichMapFunction<String, Integer>() {

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                System.out.println("子任务编号=" + getRuntimeContext().getIndexOfThisSubtask() +
                        "，子任务名称=" + getRuntimeContext().getTaskNameWithSubtasks() + "，调用open()");
            }

            @Override
            public Integer map(String value) throws Exception {
                return Integer.parseInt(value) + 1;
            }

            @Override
            public void close() throws Exception {
                super.close();
                System.out.println("子任务编号=" + getRuntimeContext().getIndexOfThisSubtask() +
                        "，子任务名称=" + getRuntimeContext().getTaskNameWithSubtasks() + "，调用close()");
            }
        });

        map.print();


        env.execute();
    }
}
