package com.cintel.windows;

import com.cintel.beans.SensorReading;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 时间窗口
 * 因为15秒才到一个窗口的时间，读文本的话会没有输出
 */
public class WindosTest1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 读取sopket文本数据流   nc -lk 6666   然后输入
        // sonser_1,234242342,23.4
        // sonser_2,1031456314,43.8
        DataStream<String> inputStream = env.socketTextStream("192.168.128.131", 6666);

        // 转换成pojo
        DataStream<SensorReading> sensorReadingStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });


        // 简单做累加示例
        DataStream<Integer> resultStream = sensorReadingStream.keyBy("id")
                .timeWindow(Time.seconds(15))
                .aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {
                    // 初始化要返回的值
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    // 如何累加
                    @Override
                    public Integer add(SensorReading sensorReading, Integer value) {
                        return value + 1;
                    }

                    // 返回结果
                    @Override
                    public Integer getResult(Integer value) {
                        return value;
                    }

                    // session会 用到合并，这里已经做了keyby 不会用到，但是可以写上
                    @Override
                    public Integer merge(Integer a, Integer b) {
                        return a + b;
                    }
                });


        resultStream.print();

        env.execute();

    }
}
