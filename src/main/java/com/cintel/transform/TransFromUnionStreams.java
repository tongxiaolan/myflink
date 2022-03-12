package com.cintel.transform;

import com.cintel.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;

/**
 * @author 小懒
 * @create 2022/3/8
 * union
 */
public class TransFromUnionStreams {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从文件中读取数据
        DataStream<String> fileStream = env.readTextFile("F:\\work\\workspaces\\idea\\wp3\\myflink\\src\\main\\resources\\sensors.txt");

        // 转换成pojo
       DataStream<SensorReading> sensorReadingStream = fileStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] fields = s.split(",");
                return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
            }
        });

       // 1 分流，以30°为界限，分为2条流
        SplitStream<SensorReading> splitStream = sensorReadingStream.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading sensorReading) {
                return sensorReading.getTemperature() > 30 ? Collections.singletonList("high") : Collections.singletonList("low");
            }
        });


        DataStream<SensorReading> highStream = splitStream.select("high");
        DataStream<SensorReading> lowStream = splitStream.select("low");
        DataStream<SensorReading> allStream = splitStream.select("high","low");


       // union 合流
        DataStream<SensorReading> unionStream = highStream.union(lowStream, allStream);
        unionStream.print();


        env.execute();

    }

}