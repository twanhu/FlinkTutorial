package com.twanhu.apitest.processfunction;

import com.twanhu.apitest.beans.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class ProcessTest1_ApplicationCase {
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

        // 测试KeyedProcessFunction,先分组然后自定义处理
        dataStream.keyBy("id")
                .process(new TempConsIncreWarning(10))
                .print();

        env.execute();
    }

    // 实现自定义处理函数，检测一段时间内的温度连续上升，输出报警
    public static class TempConsIncreWarning extends KeyedProcessFunction<Tuple, SensorReading, String> {
        // 定义私有属性，当前统计的时间间隔
        private Integer interval;

        public TempConsIncreWarning(Integer interval) {
            this.interval = interval;
        }

        // 定义状态，保存上一次的温度值，定时器时间戳
        private ValueState<Double> lastTempState;
        private ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp", Double.class,Double.MIN_VALUE));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-ts", Long.class));
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
            // 取出状态
            Double lastTemp = lastTempState.value();
            Long timerTs = timerTsState.value();

            // 如果温度上升并且没有定时器，注册10秒后的定时器，开始等待
            if (value.getTemperature() > lastTemp && timerTs == null) {
                // 计算出定时器时间戳
                // timerService(): 用于查询时间和注册计时器     currentProcessingTime(): 返回当前处理时间戳（毫秒）
                long ts = ctx.timerService().currentProcessingTime() + interval * 1000L;
                // registerProcessingTimeTimer(): 注册触发定时器的时间
                ctx.timerService().registerProcessingTimeTimer(ts);
                // 更新定时器时间戳
                timerTsState.update(ts);
            }
            // 如果温度下降，那么删除定时器
            else if (value.getTemperature() < lastTemp && timerTs != null) {
                // deleteProcessingTimeTimer(): 删除具有给定触发时间的计时器,此方法仅在此类计时器之前已注册且尚未过期时有效
                ctx.timerService().deleteProcessingTimeTimer(timerTs);
                // 删除状态
                timerTsState.clear();
            }

            // 更新温度状态
            lastTempState.update(value.getTemperature());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发，输出报警信息
            out.collect("传感器" + ctx.getCurrentKey().getField(0) + "温度值连续" + interval + "s上升");
            // 删除状态
            timerTsState.clear();
        }

        @Override
        public void close() throws Exception {
            // 删除状态
            lastTempState.clear();
        }
    }
}
