package com.study.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Hliang
 * @create 2023-08-12 12:51
 */
public class DataGeneratorDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 数据生成器会通过count计算每个并行度的随机数生成范围
        // count/并行度
        // 100 / 2
        // 那么第一个并行度就是从0~49递增生成
        // 第二个并行度就是从50-100递增生产
        env.setParallelism(2);

        /*
        参数解读：
        第一个参数GeneratorFunction：是一个接口，我们需要它的实现类（泛型为Long,xxx；输入泛型固定为Long，不能修改，输出泛型可以自定义），
                                    同时重写方法map，map方法的固定输入参数是Long类型，不能进行修改
        第二个参数count：表示随机生成的数据的最大值，他会从某个值进行顺序递增
        第三个参数rateLimiterStrategy：表示生成器的数据生成速率策略，可以自己选择
        第四个参数TypeInformation：输出值得类型，要和第一个参数GeneratorFunction输出泛型一致
         */
        DataGeneratorSource<String> dataGeneratorSource = new DataGeneratorSource<>(
                new GeneratorFunction<Long, String>() {
                    @Override
                    public String map(Long value) throws Exception {

                        return "Number：" + value;
                    }
                },
                100,
                RateLimiterStrategy.perSecond(3),
                Types.STRING
        );

        DataStreamSource<String> source = env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "dataGeneratorSource");
        source.print();

        env.execute();
    }
}
