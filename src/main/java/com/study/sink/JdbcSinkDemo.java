package com.study.sink;

import com.study.bean.WaterSensor;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author Hliang
 * @create 2023-08-14 17:35
 */
public class JdbcSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop102", 7777);
        SingleOutputStreamOperator<WaterSensor> mappedDS = dataStreamSource.map(value -> {
            String[] split = value.split(",");
            return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
        });

        /**
         * TODO 构建jdbc连接sink函数  (这里还是flink1.12之前的addSink写法，并没有过时)
         */
        SinkFunction<WaterSensor> jdbcSinkFunction = JdbcSink.sink("insert into ws values (?,?,?)", new JdbcStatementBuilder<WaterSensor>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, WaterSensor value) throws SQLException {
                        preparedStatement.setString(1, value.getId());
                        preparedStatement.setLong(2, value.getTs());
                        preparedStatement.setInt(3, value.getVc());
                    }
                },
                // jdbc执行选项
                JdbcExecutionOptions.builder()
                        .withMaxRetries(3) // 重试次数：3次
                        .withBatchSize(100) // 批次大小：条数
                        .withBatchIntervalMs(3000) // 批次时间
                        .build(),
                // jdbc连接选项
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://hadoop102:3306/test?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=UTF-8") // 连接url
                        .withUsername("root") // 用户名
                        .withPassword("123456") // 密码
                        .withConnectionCheckTimeoutSeconds(60) // 重试超时时间
                        .build()

        );

        mappedDS.addSink(jdbcSinkFunction);

        env.execute();
    }
}
