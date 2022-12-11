package com.twanhu.apitest.tableapi.udf;

import com.twanhu.apitest.beans.SensorReading;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

public class UdfTest2_TableFunction {
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

        // 4、自定义表函数（一进多出，返回一个表），实现将id拆分，并输出 (word,length)
        Split split = new Split("_");
        // 需要在环境中注册UDF
        tableEnv.registerFunction("split", split);

        // 4.1 table API
        // joinLateral(): 将原表与炸裂出来的表进行 侧连接
        Table resultTable = sensorTable.joinLateral("split(id) as (word, length)").select("id, ts, word, length");

        // 4.2、SQL
        tableEnv.createTemporaryView("sensor", sensorTable);
        Table resultSqlTable = tableEnv.sqlQuery("select id, ts, word, length " +
                "from sensor, lateral table(split(id)) as splitid(word, length)");

        // 打印输出
        tableEnv.toAppendStream(resultTable, Row.class).print("result");
        tableEnv.toAppendStream(resultSqlTable, Row.class).print("sql");

        env.execute();
    }

    // 实现自定义TableFunction
    public static class Split extends TableFunction<Tuple2<String, Integer>> {
        // 定义属性，分隔符
        private String separator;

        public Split(String separator) {
            this.separator = separator;
        }

        // 必须实现一个eval方法，且没有返回值
        public void eval(String str) {
            for (String s : str.split(separator)) {
                // collect(): 发出输出行
                collect(new Tuple2<>(s, s.length()));
            }
        }
    }
}



























