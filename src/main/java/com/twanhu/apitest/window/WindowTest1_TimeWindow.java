package com.twanhu.apitest.window;

import com.twanhu.apitest.beans.SensorReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class WindowTest1_TimeWindow {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置执行任务的并行度
        env.setParallelism(1);

        // 从socket文件读取数据
        DataStream<String> inputStream = env.socketTextStream("master", 7777);

        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 时间窗口测试，按照时间进行滚动或滑动
        // 1、增量聚合函数
        DataStream<Integer> resultStream = dataStream.keyBy("id")
                // 滚动窗口，每 15s 滚动一次并计算输出结果一次，每一次计算的 window 范围是 15s 内的所有元素
                .timeWindow(Time.seconds(15))
                // aggregate() : 以增量方式聚合值，并将状态保持为每个键和窗口一个累加器
                // AggregateFunction<IN, ACC, OUT> IN:聚合值的类型（输入值） ACC:累加器的类型（中间聚合状态） OUT:聚合结果的类型
                .aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {

                    // 创建新的累加器，开始新的聚合
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    // 将给定输入值添加到给定累加器，返回新的累加器值
                    @Override
                    public Integer add(SensorReading value, Integer accumulator) {
                        return accumulator + 1;
                    }

                    // 从累加器获取聚合结果
                    @Override
                    public Integer getResult(Integer accumulator) {
                        return accumulator;
                    }

                    // 多个分区累加器如何合并
                    @Override
                    public Integer merge(Integer a, Integer b) {
                        return a + b;
                    }
                });

        // 2、全窗口函数
        DataStream<Tuple3<String, Long, Integer>> resultStream2 = dataStream.keyBy("id")
                // 滑动窗口，每 5s 滑动一次并计算输出结果一次，每一次计算的 window 范围是 15s 内的所有元素
                .timeWindow(Time.seconds(15), Time.seconds(5))
                // WindowFunction<IN, OUT, KEY, W>  IN:输入值的类型 OUT:输出值的类型 KEY:key的类型 W:可应用此窗口功能的窗口类型
                .apply(new WindowFunction<SensorReading, Tuple3<String, Long, Integer>, Tuple, TimeWindow>() {

                    // 计算窗口，不输出任何元素或多个元素
                    @Override
                    public void apply(
                            Tuple tuple,  // 为其计算此窗口的关键字，分组Key，封装到元组中
                            TimeWindow window,    // 时间窗口，获取startTime和endTime
                            Iterable<SensorReading> input,    // 窗口中的数据
                            Collector<Tuple3<String, Long, Integer>> out    // 输出窗口的数据
                    ) throws Exception {

                        // getField() : 获取指定字段
                        String id = tuple.getField(0);
                        // getEnd() : 获取此窗口的结束时间戳
                        Long windowEnd = window.getEnd();
                        Integer count = IteratorUtils.toList(input.iterator()).size();
                        out.collect(new Tuple3<>(id, windowEnd, count));
                    }
                });

        // 3、其他 API
        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("late") {
        };

        DataStream<SensorReading> sumStream = dataStream.keyBy("id")
                // 15s 输出结果但不关闭窗口
                .timeWindow(Time.seconds(15))
                // 15s 输出结果后等待 1 分钟后关闭窗口
                // allowedLateness() : 设置允许元素延迟的时间，超过指定时间之后的元素将被丢弃
                .allowedLateness(Time.minutes(1))
                // sideOutputLateData() : 将延迟到达的数据发送到由给定OutputTag标识的侧输出流，最后在合并数据
                .sideOutputLateData(outputTag)
                .sum("temperature");

        sumStream.print();

        env.execute();
    }
}
