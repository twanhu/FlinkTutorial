package com.twanhu.apitest.sink;

import com.twanhu.apitest.beans.SensorReading;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.util.Properties;

public class SinkTest1_kafka {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置执行任务的并行度
        env.setParallelism(3);

        // 配置Kafka Producer相关的属性，Properties : 用于读取Java的配置文件，其配置文件常为.properties文件，是以键值对的形式进行参数配置的
        Properties producerProperties = new Properties();
        producerProperties.setProperty("zookeeper.connect", "master:2181,slave1:2181,slave2:2181");
        producerProperties.setProperty("bootstrap.servers", "master:9092,slave1:9092,slave2:9092");
        // 序列化
        producerProperties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProperties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 配置Kafka Consumer相关的属性
        Properties consumerProperties = new Properties();
        consumerProperties.setProperty("zookeeper.connect", "master:2181,slave1:2181,slave2:2181");
        consumerProperties.setProperty("bootstrap.servers", "master:9092,slave1:9092,slave2:9092");
        consumerProperties.setProperty("group.id", "consumer");
        // 反序列化
        consumerProperties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProperties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProperties.setProperty("auto.offset.reset", "latest");

        // 从kafka读取数据,kafka传输数据的格式为字节序列，SimpleStringSchema 对其序列化
        DataStream<String> inputStream = env.addSource(new FlinkKafkaConsumer011<String>("sensor", new SimpleStringSchema(), consumerProperties));

        // 转换成SensorReading类型
        DataStream<String> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2])).toString();
        });

        // 将数据发送到kafka,kafka传输数据的格式为字节序列，SimpleStringSchema 对其序列化
        dataStream.addSink(new FlinkKafkaProducer011<String>("sinktest", new SimpleStringSchema(),producerProperties));

        env.execute();
    }
}
