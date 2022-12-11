package com.twanhu.apitest.tableapi;

import com.twanhu.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class TableTest1_Example {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1、读取数据
        DataStream<String> inputStream = env.readTextFile("dataset/sensor.txt");

        // 2、转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 3、创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 4、将给定的数据流转换为表
        Table dataTable = tableEnv.fromDataStream(dataStream);

        // 5、调用Table API进行转换操作
        Table resultTable = dataTable.select("id, temperature").where("id = 'sensor_1'");

        // 6、SQL
        // 创建视图
        tableEnv.createTemporaryView("sensor", dataTable);
        Table resultSqlTable = tableEnv.sqlQuery("select id, temperature from sensor where id = 'sensor_1'");

        // 打印输出
//         toAppendStream(): 将给定表转换为指定类型的追加数据流
        tableEnv.toAppendStream(resultTable, Row.class).print("result");
        tableEnv.toAppendStream(resultSqlTable, Row.class).print("sql");

        env.execute();
    }
}
