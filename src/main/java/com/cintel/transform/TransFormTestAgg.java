package com.cintel.transform;

import com.cintel.beans.SensorReading;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author 小懒
 * @create 2022/3/8
 */
public class TransFormTestAgg {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从文件中读取数据
        DataStream<String> fileStream = env.readTextFile("F:\\work\\workspaces\\idea\\wp3\\myflink\\src\\main\\resources\\sensors.txt");

        // 转换成pojo
        DataStream<SensorReading> sensorReadingStream = fileStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });


        // 分组 keyBy
        KeyedStream<SensorReading, Tuple> keyByStream = sensorReadingStream.keyBy("id");

        //sum
        SingleOutputStreamOperator<SensorReading> temperatureStream = keyByStream.sum("temperature");
        temperatureStream.print("sum");


        // max
        DataStream<SensorReading> tempMax = keyByStream.max("temperature");
        DataStream<SensorReading> tempMaxBy = keyByStream.maxBy("temperature");
        tempMax.print("max");
        tempMaxBy.print("maxBy");

        // min
        DataStream<SensorReading> tempMin = keyByStream.min("temperature");
        DataStream<SensorReading> tempMinBy = keyByStream.min("temperature");
        tempMin.print("min");
        tempMinBy.print("minBy");


        // reduce
        DataStream<SensorReading> reduce = keyByStream.reduce(new ReduceFunction<SensorReading>() {
            @Override
            public SensorReading reduce(SensorReading sensor1, SensorReading sensor2) throws Exception {
                return new SensorReading(sensor1.getId(), sensor2.getTimestamp(), Math.max(sensor1.getTemperature(), sensor2.getTemperature()));
            }
        });
        reduce.print("reduce");

        env.execute();
    }

}