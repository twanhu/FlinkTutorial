package com.twanhu.apitest.window;

import com.twanhu.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

public class WindowTest3_EventTimeWindow {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 设置从该环境创建的所有流的时间特性为EventTime（事件创建的时间，数据自带的时间）
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // getConfig(): 获取配置对象  setAutoWatermarkInterval(): 设置 Watermark 间隔生成的时间(毫秒)，每 100ms 生成一个 Watermark
        env.getConfig().setAutoWatermarkInterval(100);

        // socket文本流
        DataStream<String> inputStream = env.socketTextStream("master", 7777);

        /**
         * 当到达特定watermark时,认为在watermark之前的数据已经全部达到(即使后面还有延迟的数据), 可以触发窗口计算
         * watermark本质上是一个时间戳，且是动态变化和单调递增的，会根据当前最大事件时间产生
         * watermark = 进入 Flink 窗口的最大的事件时间(maxEventTime) — 指定的延迟时间(t)
         * 当watermark时间戳大于等于窗口结束时间时，意味着窗口结束，需要触发窗口计算
         */

        // 转换成SensorReading类型,分配时间戳和watermark
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

        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("late") {
        };

        // 基于事件时间的开窗聚合，统计15秒内温度的最小值
        SingleOutputStreamOperator<SensorReading> minTempStream = dataStream.keyBy("id")
                // 时间滚动窗口
                .timeWindow(Time.seconds(5))
                // allowedLateness() : 设置允许元素延迟的时间，超过指定时间之后的元素将被丢弃
                .allowedLateness(Time.minutes(1))
                // sideOutputLateData() : 将延迟到达的数据发送到由给定OutputTag标识的侧输出流，最后在合并数据
                .sideOutputLateData(outputTag)
                .minBy("temperature");

        minTempStream.print("minTemp");
        minTempStream.getSideOutput(outputTag).print("late");

        env.execute();
    }
}
