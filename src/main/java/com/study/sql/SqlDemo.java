package com.study.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author Hliang
 * @create 2023-09-05 23:25
 */
public class SqlDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // TODO 1、创建表环境
        // TODO 创建方法一：
//        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
//        TableEnvironment tableEnv = TableEnvironment.create(environmentSettings);
        // TODO 创建方法二
        // 已经是Stream，所以默认已经就是流环境了，不用再通过EnvironmentSettings来控制环境是 流 还是 批
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2、创建表，通过executeSql方法进行创建表
        tableEnv.executeSql("CREATE TABLE source ( \n" +
                "    id INT, \n" +
                "    ts BIGINT, \n" +
                "    vc INT\n" +
                ") WITH ( \n" +
                "    'connector' = 'datagen', \n" +
                "    'rows-per-second'='1', \n" +
                "    'fields.id.kind'='random', \n" +
                "    'fields.id.min'='1', \n" +
                "    'fields.id.max'='10', \n" +
                "    'fields.ts.kind'='sequence', \n" +
                "    'fields.ts.start'='1', \n" +
                "    'fields.ts.end'='1000000', \n" +
                "    'fields.vc.kind'='random', \n" +
                "    'fields.vc.min'='1', \n" +
                "    'fields.vc.max'='100'\n" +
                ");\n");

        tableEnv.executeSql("CREATE TABLE sink (\n" +
                "    id INT, \n" +
                "    sumVC INT \n" +
                ") WITH (\n" +
                "'connector' = 'print'\n" +
                ");\n");

        // TODO 3、执行查询 执行查询结果也是一个动态表
        // 3.1 用sql来查询
//        Table table = tableEnv.sqlQuery("select id,sum(vc) as sumVC from source where id > 5 group by id;");
//        tableEnv.createTemporaryView("tmp",table); // 这是一个虚拟表，是我们创建的视图
//        tableEnv.sqlQuery("select * from tmp");

        // 4.1 用table api来查询
        Table table = tableEnv.from("source");
        Table result = table
                .where($("id").isGreater(5))
                .groupBy($("id"))
                .select($("id"), $("vc").sum().as("sumVC"));


        // TODO 4、输出表
        // 4.1 sql用法 （使用executeSql）
//        tableEnv.executeSql("insert into sink select * from tmp");
        result.executeInsert("sink");

    }
}
