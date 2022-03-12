package com.cintel.transform;

import com.cintel.beans.SensorReading;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

/**
 * @author 小懒
 * @create 2022/3/9
 * @since 1.0.0
 */
public class SinkKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> inputStream = env.readTextFile("F:\\work\\workspaces\\idea\\wp3\\myflink\\src\\main\\resources\\words.txt");

        // 转换成pojo
        DataStream<String> sensorReadingStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2])).toString();
        });

        // 写入kafka
        sensorReadingStream.addSink(new FlinkKafkaProducer011<String>("192.168.128.131:9092", "sensorSinkTest", new SimpleStringSchema()));


        env.execute();
    }

}