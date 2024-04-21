package com.study.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * TODO 文件当作数据源
 * @author Hliang
 * @create 2023-08-12 11:30
 */
public class FileSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // flink1.12之后新的source架构
        // 参数解读
        // 第一个参数source：表示数据源
        // 第二个参数WaterMarkStrategy：表示水位线策略
        // 第三个参数sourceName：表示数据源名称
        // 创建一个FileSource （一般来说数据源是什么，那么其对应封装好的Source就是xxxSource）
        FileSource<String> fileSource = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path("input/word.txt")).build();
        DataStreamSource<String> source = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "fileSource");
        source.print();

        env.execute();
    }
}
