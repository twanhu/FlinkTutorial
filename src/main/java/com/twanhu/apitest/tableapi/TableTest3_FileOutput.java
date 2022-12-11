package com.twanhu.apitest.tableapi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

public class TableTest3_FileOutput {
    public static void main(String[] args) throws Exception {
        // 1、创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2、表的创建，连接外部系统，读取数据
        // 2.1、读取文件
        String filePath = "dataset/sensor.txt";
        // connect(): 定义表的数据来源或从描述中创建表
        tableEnv.connect(new FileSystem().path(filePath))
                // withFormat(): 指定如何从连接器读取数据的格式
                .withFormat(new Csv())
                // withSchema(): 定义表结构
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamp", DataTypes.BIGINT())
                        .field("temp", DataTypes.DOUBLE()))
                // 创建临时表
                .createTemporaryTable("inputTable");

        // from(): 读取注册表并返回结果表
        Table inputTable = tableEnv.from("inputTable");

        // 3、查询转换
        // 3.1、Table API
        // 简单转换
        Table resultTable = inputTable.select("id, temp").filter("id = 'sensor_1'");
        // 聚合统计
        Table aggTable = inputTable.groupBy("id").select("id, id.count as count, temp.avg as avgTemp");

        // 3.2、SQL
        tableEnv.sqlQuery("select id, temp from inputTable where id = 'senosr_6'");
        Table sqlAggTable = tableEnv.sqlQuery("select id, count(id) as cnt, avg(temp) as avgTemp from inputTable group by id");

        // 4、输出到文件
        // 连接外部文件注册输出表
        String outputPath = "dataset/out.txt";
        // connect(): 定义表的数据来源或从描述中创建表
        tableEnv.connect(new FileSystem().path(outputPath))
                // withFormat(): 指定如何从连接器读取数据的格式
                .withFormat(new Csv())
                // withSchema(): 定义表结构
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("temperature", DataTypes.DOUBLE()))
                .createTemporaryTable("outputTable");

        // 将表写入在指定路径下注册的TableSink
        resultTable.insertInto("outputTable");
//        aggTable.insertInto("outputTable");

        env.execute();
    }
}
