package com.twanhu.apitest.processfunction;

import com.twanhu.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class ProcessTest2_SideOuptCase {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // socket文本流
        DataStream<String> inputStream = env.socketTextStream("master", 3333);

        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 定义一个OutputTag，用来表示侧输出流，低温流
        OutputTag<SensorReading> lowtempTag = new OutputTag<SensorReading>("lowTemp") {
        };

        // 测试KeyedProcessFunction,先分组然后自定义处理
        final SingleOutputStreamOperator<Object> highTempStream = dataStream.process(new ProcessFunction<SensorReading, Object>() {
            @Override
            public void processElement(SensorReading value, ProcessFunction<SensorReading, Object>.Context ctx, Collector<Object> out) throws Exception {
                // 判断温度，大于30度高温流输出到主流；小于30低温流输出到侧输出流
                if (value.getTemperature() > 30) {
                    out.collect(value);
                } else {
                    // ctx.output(): 向OutputTag标识的侧输出发出记录
                    ctx.output(lowtempTag, value);
                }
            }
        });

        highTempStream.print("high-temp");
        highTempStream.getSideOutput(lowtempTag).print("low-temp");

        env.execute();
    }
}
