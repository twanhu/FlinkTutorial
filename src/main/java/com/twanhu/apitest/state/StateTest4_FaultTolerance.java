package com.twanhu.apitest.state;

import com.twanhu.apitest.beans.SensorReading;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StateTest4_FaultTolerance {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        /**
         * 状态后端（State Backends）
         * • 每传入一条数据，有状态的算子任务都会读取和更新状态
         * • 由于有效的状态访问对于处理数据的低延迟至关重要，因此每个并行任务都会在本地维护其状态，以确保快速的状态访问
         * • 状态的存储、访问以及维护，由一个可插入的组件决定，这个组件就叫做状态后端（state backend）
         * • 状态后端主要负责两件事：本地的状态管理，以及将检查点（checkpoint）状态写入远程存储
         *
         *  Flink提供不同的状态后端（State Backend）来区分状态的存储方式和存储位置。
         *  Flink状态可以存储在java堆内存内或者内存之外。通过状态后端的设置，Flink允许应用持有大容量的状态。
         *  开发者可以在不改变应用逻辑的情况下设置状态后端
         */
        // 1、状态后端配置
        // env.setStateBackend(): 设置描述如何存储和检查点运算符状态的状态后端
        env.setStateBackend(new MemoryStateBackend());  // 内存级的状态后端，会将键控状态作为内存中的对象进行管理
        // 将检查点（checkpoint）存到远程的持久化文件系统（hdfs）上，而对于本地状态，跟 MemoryStateBackend 一样
        env.setStateBackend(new FsStateBackend(""));
        // 将所有状态序列化后，存入本地的 RocksDB 中存储
        env.setStateBackend(new RocksDBStateBackend(""));

        /**
         * 检查点：将所有任务都处理完同一份数据之后的时间点做一份保存
         */
        // 2、检查点配置
        // 给定的时间间隔内定期绘制检查点。状态将存储在配置的状态后端中,如果出现故障，将从最新完成的检查点重新启动数据流
        env.enableCheckpointing(300);

        // 3、设置重新启动策略配置
        // 固定延迟重启
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,10000L));
        // 失败率重启
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.minutes(10),Time.minutes(1)));

        // socket文本流
        DataStream<String> inputStream = env.socketTextStream("master", 3333);

        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        dataStream.print();
        env.execute();
    }
}
