package com.twanhu.apitest.tableapi;

import com.twanhu.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.types.Row;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

public class TableTest5_TimeAndWindow {
    public static void main(String[] args) throws Exception {
        // 1、创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 设置从该环境创建的所有流的时间特性为EventTime（事件创建的时间，数据自带的时间）
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2、读入文件数据，得到DataStream
        DataStream<String> inputStream = env.readTextFile("dataset/sensor.txt");

        // 3、转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
                    String[] fields = line.split(",");
                    return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
                })
                // assignTimestampsAndWatermarks(): 为数据流中的元素分配时间戳，并定期创建Watermarks
                // 设置Watermarks最大的延迟时间为 2s
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {
                    // 提取事件时间戳，数据自带的时间（毫秒）
                    @Override
                    public long extractTimestamp(SensorReading element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        // 4、将流转换成表，定义时间特性
        // 使用 .proctime 指定处理时间字段
//        Table dataTable = tableEnv.fromDataStream(dataStream, "id, timestamp as ts, temperature as temp, pt.proctime");
        // 使用 .rowtime 可以定义事件时间属性
        Table dataTable = tableEnv.fromDataStream(dataStream, "id, timestamp as ts, temperature as temp, rt.rowtime");

        tableEnv.createTemporaryView("sensor", dataTable);

        // 5、窗口操作
        // 5.1、Group window （分组窗口）
        // table API
        // 定义滚动窗口大小为 10s，分组的时间属性为rt，别名为 tw
        Table resultTable = dataTable.window(Tumble.over("10.seconds").on("rt").as("tw"))
                .groupBy("id, tw")  // 按照字段 id 和窗口 tw 分组
                .select("id, id.count, temp.avg, tw.end");  // 聚合，tw.end 获取窗口的结束时间

        // SQL
        // tumble_end(): 窗口结束的时间，tumble(): 定义滚动窗口，第一个参数是时间字段，第二个参数是窗口大小
        Table resultSqlTable = tableEnv.sqlQuery("select id, count(id) as cnt, avg(temp) as avgTemp,tumble_end(rt, interval '10' second) "
                + "from sensor group by id, tumble(rt, interval '10' second)");

        // 5.2、Over Window
        // table API
        // 定义窗口，窗口大小为 当前行到前两行
        Table overResult = dataTable.window(Over.partitionBy("id").orderBy("rt").preceding("2.rows").as("ow"))
                .select("id, rt, id.count over ow, temp.avg over ow");

        // SQL
        Table overSqlResult = tableEnv.sqlQuery("select id, rt, count(id) over ow, avg(temp) over ow" +
                " from sensor " +
                "window ow as (partition by id order by rt rows between 2 preceding and current row)");

//        dataTable.printSchema();
//        tableEnv.toAppendStream(resultTable, Row.class).print("result");
//        tableEnv.toAppendStream(resultSqlTable, Row.class).print("sql");
//        tableEnv.toAppendStream(overResult, Row.class).print("result");
        tableEnv.toRetractStream(overSqlResult, Row.class).print("sql");

        env.execute();
    }
}
