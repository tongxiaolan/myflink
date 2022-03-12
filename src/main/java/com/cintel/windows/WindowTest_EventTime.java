package com.cintel.windows;

import com.cintel.beans.SensorReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 时间语义
 */
public class WindowTest_EventTime {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);


        // 读取sopket文本数据流   nc -lk 6666   然后输入
        // sonser_1,234242342,23.4
        // sonser_2,1031456314,43.8
        DataStream<String> inputStream = env.socketTextStream("192.168.128.131", 6666);

        // 转换成pojo
        DataStream<SensorReading> sensorReadingStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });

        // 乱序数据设置事件时间并设置延迟时间为3s
        sensorReadingStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(3)) {
            @Override
            public long extractTimestamp(SensorReading element) {
                return element.getTimestamp();
            }
        });

        // 升序数据设置事件时间和watermark
        sensorReadingStream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<SensorReading>() {
            @Override
            public long extractAscendingTimestamp(SensorReading element) {
                return element.getTimestamp();
            }
        });
    }
}
