package com.twanhu.apitest.window;

import com.twanhu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

public class WindowTest2_CountWindow {
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

        // 计数窗口测试，按照记录的个数进行滚动或滑动
        // 1、增量聚合函数,在累加的基础上来一条处理一条
        DataStream<Double> avgTempResultStream = dataStream.keyBy("id")
                // 计数窗口，每 2 条数据滑动一次并计算输出结果一次，每一次计算的 window 范围最多记录数为 15 条
                .countWindow(10, 2)
                // aggregate() : 以增量方式聚合值，并将状态保持为每个键和窗口一个累加器
                .aggregate(new MyAvgTemp());

        // 2、全窗口函数，将窗口内的全部数据统一处理
        DataStream<Double> avgTempResultStream1 = dataStream.keyBy("id")
                // 计数窗口，每 10 条数据滑动一次并计算输出结果一次，每一次计算的 window 范围最多记录数为 10 条
                .countWindow(10)
                .apply(new MyAvgTemp1());

        avgTempResultStream1.print();

        env.execute();
    }

    // AggregateFunction<IN, ACC, OUT> IN:聚合值的类型（输入值） ACC:累加器的类型（中间聚合状态） OUT:聚合结果的类型
    public static class MyAvgTemp implements AggregateFunction<SensorReading, Tuple2<Double, Integer>, Double> {

        // 创建新的累加器，开始新的聚合
        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0.0, 0);
        }

        // 将给定输入值添加到给定累加器，返回新的累加器值
        @Override
        public Tuple2<Double, Integer> add(SensorReading value, Tuple2<Double, Integer> accumulator) {
            return new Tuple2<>(accumulator.f0 + value.getTemperature(), accumulator.f1 + 1);
        }

        // 从累加器获取聚合结果
        @Override
        public Double getResult(Tuple2<Double, Integer> accumulator) {
            return accumulator.f0 / accumulator.f1;
        }

        // 多个分区累加器如何合并
        @Override
        public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> a, Tuple2<Double, Integer> b) {
            return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
        }
    }

    public static class MyAvgTemp1 implements WindowFunction<SensorReading, Double, Tuple, GlobalWindow> {

        @Override
        public void apply(Tuple tuple, GlobalWindow window, Iterable<SensorReading> input, Collector<Double> out) throws Exception {
            // 定义累加器的初始值
            double accumulator = 0.0;
            int count = 0;

            for (SensorReading sensorReading : input) {
                accumulator += sensorReading.getTemperature();
                count++;
            }
            out.collect(accumulator / count);
        }
    }

}
