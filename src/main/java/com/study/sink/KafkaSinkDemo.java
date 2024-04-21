package com.study.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;

/**
 * @author Hliang
 * @create 2023-08-14 17:12
 */
public class KafkaSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 开启检查点
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE); // 毫秒为单位

        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop102", 7777);

        /**
         * TODO kafkaSink中的注意事项
         * TODO 1.如果采用的精确一次生产，那么必须要
         *              1）开启检查点
         *              2）指定事务id的前缀
         *              3）指定事务的超时时间  （检查点时间间隔 < 事务的超时时间 < 最大超时时间15分钟）
         *
         */
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                // 设置连接的kafka集群
                .setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
                // 设置序列化器，同时里面可以设置主题
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<String>builder()
                                .setTopic("ws")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build())
                // 指定kafka的一致性级别：精准一次、至少一次
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                // 指定事务id的前缀
                .setTransactionalIdPrefix("atguigu-")
                // 指定事务的超时时间，大于检查点时间间隔，小于max 15分钟
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,String.valueOf(10 * 60 * 1000))  // 毫秒为单位
                .build();


        dataStreamSource.sinkTo(kafkaSink);


        env.execute();
    }
}
