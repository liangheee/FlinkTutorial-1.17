package com.study.sink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;
import java.time.ZoneId;

/**
 * @author Hliang
 * @create 2023-08-14 16:00
 */
public class FIleSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // TODO 有几个并行度，那么一个Rolling滚动周期中写出的文件就会有几个
        env.setParallelism(2);

        // TODO 一定要设置检查点，每周期写入结束后，会去掉文件后缀里面的inprogress
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);

        DataGeneratorSource dataGeneratorSource = new DataGeneratorSource(new GeneratorFunction<Long, String>() {
            @Override
            public String map(Long value) throws Exception {
                return "Number：" + value;
            }
        },
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(100),
                Types.STRING
        );

        DataStreamSource dataStreamSource = env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "dataGeneratorSource");

        FileSink<String> fileSink = FileSink.<String>forRowFormat(new Path("d:/tmp"), new SimpleStringEncoder<>())
                // 文件输出配置（配置输出文件的前缀后后缀）
                .withOutputFileConfig(
                        OutputFileConfig.builder()
                                .withPartPrefix("atguigu")
                                .withPartSuffix(".log")
                                .build())
                // 文件目录分桶：如下就是按照每一个小时，一个文件目录
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd HH", ZoneId.systemDefault()))
                // 设置滚动策略 1m 或 1分钟
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withMaxPartSize(new MemorySize(1024 * 1024))
                                .withRolloverInterval(Duration.ofSeconds(60))
                                .build()
                ).build();


        DataStreamSink dataStreamSink = dataStreamSource.sinkTo(fileSink);


        env.execute();
    }
}
