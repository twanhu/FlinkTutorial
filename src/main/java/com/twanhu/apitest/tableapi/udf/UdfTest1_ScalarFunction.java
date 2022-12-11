package com.twanhu.apitest.tableapi.udf;

import com.twanhu.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

public class UdfTest1_ScalarFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1、读取数据
        DataStream<String> inputStream = env.readTextFile("dataset/sensor.txt");

        // 2、转换成 SensorReading
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 3、将流转换成表
        Table sensorTable = tableEnv.fromDataStream(dataStream, "id, timestamp as ts, temperature as temp");

        // 4、自定义标量函数（一对一），实现求id的hash值
        HashCode hashCode = new HashCode();
        // 需要在环境中注册UDF
        tableEnv.registerFunction("hashCode", hashCode);

        // 4.1 table API
        Table resultTable = sensorTable.select("id, ts, hashCode(id)");

        // 4.2、SQL
        tableEnv.createTemporaryView("sensor", sensorTable);
        Table resultSqlTable = tableEnv.sqlQuery("select id, ts, hashCode(id) from sensor");

        // 打印输出
        tableEnv.toAppendStream(resultTable, Row.class).print("result");
        tableEnv.toAppendStream(resultSqlTable, Row.class).print("sql");

        env.execute();
    }

    // 实现自定义的ScalarFunction
    public static class HashCode extends ScalarFunction {

        // 必须实现一个eval方法，且没有返回值
        public int eval(String str) {
            // hashCode(): 返回字符串的哈希代码
            return str.hashCode();
        }

    }
}
