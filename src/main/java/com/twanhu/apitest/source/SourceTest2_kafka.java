package com.twanhu.apitest.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

public class SourceTest2_kafka {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);

        // 配置kafka相关的属性，Properties : 用于读取Java的配置文件，其配置文件常为.properties文件，是以键值对的形式进行参数配置的
        Properties properties = new Properties();
        properties.setProperty("zookeeper.connect", "master:2181,slave1:2181,slave2:2181");
        properties.setProperty("bootstrap.serversc", "master:9092,slave1:9092,slave2:9092");
        // 序列化
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 从kafka读取数据,kafka传输数据的格式为字节序列，SimpleStringSchema 对其序列化
        DataStream<String> dataStream = env.addSource(new FlinkKafkaConsumer011<String>("sensor", new SimpleStringSchema(), properties));

        // 打印输出
        dataStream.print();

        // 执行任务
        env.execute();
    }
}
