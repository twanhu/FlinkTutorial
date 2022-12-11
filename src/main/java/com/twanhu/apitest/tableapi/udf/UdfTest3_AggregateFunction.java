package com.twanhu.apitest.tableapi.udf;

import com.twanhu.apitest.beans.SensorReading;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.types.Row;

public class UdfTest3_AggregateFunction {
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

        // 4、自定义聚合函数，求当前传感器的平均值
        AvgTemp avgTemp = new AvgTemp();
        // 需要在环境中注册UDF
        tableEnv.registerFunction("avgTemp", avgTemp);

        // 4.1 table API
        Table resultTable = sensorTable.groupBy("id").select("id, avgtemp(temp)");

        // 4.2、SQL
        tableEnv.createTemporaryView("sensor", sensorTable);
        Table resultSqlTable = tableEnv.sqlQuery("select id, avgTemp(temp) from sensor group by id");

        // 打印输出
        tableEnv.toRetractStream(resultTable, Row.class).print("result");
        tableEnv.toRetractStream(resultSqlTable, Row.class).print("sql");

        env.execute();
    }

    // 实现自定义的AggregateFunction
    // AggregateFunction<T, ACC>    T: 聚合结果的类型   ACC: 聚合累加器的类型
    public static class AvgTemp extends AggregateFunction<Double, Tuple2<Double, Integer>> {

        @Override
        public Double getValue(Tuple2<Double, Integer> accumulator) {
            return accumulator.f0 / accumulator.f1;
        }

        // 创建并初始化累加器
        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0.0, 0);
        }

        // 必须实现一个accumulate方法，来数据之后更新状态
        public void accumulate(Tuple2<Double, Integer> accumulator, Double temp) {
            accumulator.f0 += temp;
            accumulator.f1++;
        }
    }
}
























