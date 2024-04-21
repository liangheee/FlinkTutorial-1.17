package com.study.sink;

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

/**
 * @author Hliang
 * @create 2023-08-14 17:31
 */
public class KafkaSInkWithDIYSerialization {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);

        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop102", 7777);

        /**
         * TODO 自定义kafka生产者的序列化器步骤
         *      1）实现KafkaRecordSeializationSchema接口
         *      2）实现serialize方法
         *      3）创建对应的ProducerRecord返回
         *
         */
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
                .setRecordSerializer(new KafkaRecordSerializationSchema<String>() {
                    @Nullable
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(String element, KafkaSinkContext context, Long timestamp) {
                        String[] split = element.split(",");
                        byte[] key = split[1].getBytes(StandardCharsets.UTF_8);
                        byte[] value = element.getBytes(StandardCharsets.UTF_8);
                        return new ProducerRecord<>("ws",key,value);
                    }
                })
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("atguigu-")
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,String.valueOf(10 * 60 * 1000))  // 毫秒为单位
                .build();


        dataStreamSource.sinkTo(kafkaSink);


        env.execute();
    }
}
