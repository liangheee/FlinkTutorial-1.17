package com.study.sql;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author Hliang
 * @create 2023-09-06 0:11
 */
public class MyTableFunctionDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> strDS = env.fromElements(
                "hello flink",
                "hello scala",
                "hello spark"
        );

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Table table = tableEnv.fromDataStream(strDS,$("words"));
        tableEnv.createTemporaryView("str",table);

        // TODO 2、注册函数
        tableEnv.createTemporarySystemFunction("SplitFunction",SplitFunction.class);

        // TODO 3、调用 自定义函数
        // 3.1 交叉联结
//        tableEnv.sqlQuery("select words,word,length from str,lateral table(SplitFunction(words))").execute().print();
        // 3.2 带on true 条件的左联结
//        tableEnv.sqlQuery("select words,word,length from str left join lateral table(SplitFunction(words)) on true").execute().print();
        tableEnv.sqlQuery("select words,newWord,newLength from str left join lateral table(SplitFunction(words)) as T(newWord,newLength) on true").execute().print();
    }

    // TODO 1、继承TableFunction函数
    //   类型标注：Row包含两个字段，word和length
    @FunctionHint(output = @DataTypeHint("Row<word STRING,length INT>"))
    public static class SplitFunction extends TableFunction<Row> {
        public void eval(String str){
            String[] split = str.split(" ");
            for (String word : split) {
                collect(Row.of(word,word.length()));
            }
        }
    }
}
