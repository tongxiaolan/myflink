package com.cintel.transform;

import com.cintel.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.Collections;

/**
 * 合并流 connect
 */
public class TransFormConnectStreams {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> fileStream = env.readTextFile("D:\\workspace\\myflink\\src\\main\\resources\\sensors.txt");
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

        //转换为2条流
        DataStream<SensorReading> highStream = splitStream.select("high");
        DataStream<SensorReading> lowStream = splitStream.select("low");

        // 将高温流转化类型，用作后续的合流操作
        SingleOutputStreamOperator<Tuple2<String, Double>> warningStream = highStream.map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(SensorReading sensorReading) throws Exception {
                return new Tuple2<>(sensorReading.getId(), sensorReading.getTemperature());
            }
        });

        // 链接
        ConnectedStreams<Tuple2<String, Double>, SensorReading> connect = warningStream.connect(lowStream);

        // 对合并的流进行操作
        DataStream<Object> resultStream = connect.map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Object>() {
            @Override
            public Object map1(Tuple2<String, Double> stringDoubleTuple2) throws Exception {
                // 操作第一条流中的数据
                return new Tuple3<>(stringDoubleTuple2.f0, stringDoubleTuple2.f1, "high temp warning");
            }

            @Override
            public Object map2(SensorReading sensorReading) throws Exception {
                // 操作第二条流中的数据
                return new Tuple2<>(sensorReading.getId(), "normal");
            }
        });

        resultStream.print();

        env.execute();
    }
}
