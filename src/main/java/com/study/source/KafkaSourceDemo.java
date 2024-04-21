package com.study.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

/**
 * TODO Kafka当作数据源
 * @author Hliang
 * @create 2023-08-12 11:30
 */
public class KafkaSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从kafka中读取数据
        /*
        Kafka本身维护者着一套重置offsets的策略：
        auto.reset.offsets：
                earliest：如果当前有offsets，那么继续从该offsets消费；如果当前没有offsets，则从最早开始消费
                latest：如果当前有offsets，那么继续从该offsets消费；如果当前没有offsets，则从最新数据开始消费
        TODO flink也维护着自己的一套offsets消费策略：
                earliest：直接从最早开始消费
                latest：直接从最新开始消费
        TODO 注意kafka和flink维护的offsets重置策略是不一样的
         */
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
                .setGroupId("atguigu")
                .setTopics("topic_1")
                .setValueOnlyDeserializer(new SimpleStringSchema())
//                .setStartingOffsets(OffsetsInitializer.earliest())
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .build();
        DataStreamSource<String> source = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource");
        source.print();
        env.execute();
    }
}
