package com.twanhu.apitest.transform;

import com.twanhu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.configuration.Configuration;

public class TransformTest5_RichFunction {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置执行任务的并行度
        env.setParallelism(3);

        // 从文件读取数据
        DataStream<String> inputStream = env.readTextFile("dataset/sensor.txt");

        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        DataStream<Tuple2<String, Integer>> resultStream = dataStream.map(new MyMapper());

        resultStream.print();

        env.execute();
    }

    /**
     * “富函数”是 DataStream API 提供的一个函数类的接口，所有 Flink 函数类都
     * 有其 Rich 版本。它与常规函数的不同在于，可以获取运行环境的上下文，并拥有一
     * 些生命周期方法，所以可以实现更复杂的功能
     *
     * Rich Function 有一个生命周期的概念。典型的生命周期方法有：
     * open()方法是 rich function 的初始化方法，当一个算子例如 map 或者 filter被调用之前 open()会被调用。
     * close()方法是生命周期中的最后一个调用的方法，做一些清理工作。
     * getRuntimeContext()方法提供了函数的 RuntimeContext 的一些信息，例如函数执行的并行度，任务的名字，以及 state 状态
     */
    // 实现自定义富函数类
    public static class MyMapper extends RichMapFunction<SensorReading, Tuple2<String, Integer>> {

        @Override
        public Tuple2<String, Integer> map(SensorReading value) throws Exception {
            // getIndexOfThisSubtask() : 获取此并行子任务的编号。编号从0开始，一直到并行度-1
            return new Tuple2<>(value.getId(), getRuntimeContext().getIndexOfThisSubtask());
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化工作，一般是定义状态，或者建立数据库连接
            System.out.println("open");
        }

        @Override
        public void close() throws Exception {
            // 一般是关闭连接和清空状态的收尾操作
            System.out.println("close");
        }
    }
}













