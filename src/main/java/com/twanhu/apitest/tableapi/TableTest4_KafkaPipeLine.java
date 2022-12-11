package com.twanhu.apitest.tableapi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

public class TableTest4_KafkaPipeLine {
    public static void main(String[] args) throws Exception {
        // 1、创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2、连接kafka，读取数据
        tableEnv.connect(new Kafka()
                        .version("0.11")
                        .topic("sensor")
                        .property("zookeeper.connect", "master:2181,slave1:2181,slave2:2181")
                        .property("bootstrap.servers", "master:9092,slave1:9092,slave2:9092"))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamp", DataTypes.BIGINT())
                        .field("temp", DataTypes.DOUBLE()))
                .createTemporaryTable("inputTable");

        // from(): 读取注册表并返回结果表
        Table sensorTable = tableEnv.from("inputTable");

        // 3、查询转换
        // 简单转换
        Table resultTable = sensorTable.select("id, temp").filter("id = 'sensor_1'");
        // 聚合统计
        Table aggTable = sensorTable.groupBy("id").select("id, id.count as count, temp.avg as avgTemp");

        // 4、建立kafka连接，输出到不同的topic下
        tableEnv.connect(new Kafka()
                        .version("0.11")
                        .topic("sinktest")
                        .property("zookeeper.connect", "master:2181,slave1:2181,slave2:2181")
                        .property("bootstrap.servers", "master:9092,slave1:9092,slave2:9092"))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("temperature", DataTypes.DOUBLE()))
                .createTemporaryTable("outputTable");

        // 将表写入在指定路径下注册的TableSink
        resultTable.insertInto("outputTable");

        env.execute();
    }
}
























