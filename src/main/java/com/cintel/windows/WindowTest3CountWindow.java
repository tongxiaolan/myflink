package com.cintel.windows;

import com.cintel.beans.SensorReading;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

public class WindowTest3CountWindow {
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

        SingleOutputStreamOperator resultStream = sensorReadingStream.keyBy("id")
                .countWindow(10, 2)
                .aggregate(new MyAggregate());

        resultStream.print();


        // 定义标签
        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("late") {
        };

        // 其他可选api
        SingleOutputStreamOperator<SensorReading> sumStream = sensorReadingStream.keyBy("id")
                .timeWindow(Time.hours(1))
                .allowedLateness(Time.minutes(2)) //允许数据迟到2分钟 只对时间时间窗口有效
                .sideOutputLateData(outputTag) // 2分钟以后到的流 放到侧流里
                .sum("temperature");


        // 处理侧输出流里的数据  SingleOutputStreamOperator里的方法
        sumStream.getSideOutput(outputTag).print("late ");

        env.execute();

    }


    // 自定义求均值的聚合方法                                      //输入                综合      个数         返回值
    public static class MyAggregate implements AggregateFunction<SensorReading, Tuple2<Double, Integer>, Double> {
        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0.0, 0);
        }

        @Override
        public Tuple2<Double, Integer> add(SensorReading sensorReading, Tuple2<Double, Integer> t1) {
            // 当前温度和新sensorReading的温度累加，个数+1
            return new Tuple2<>(t1.f0 + sensorReading.getTemperature(), t1.f1 + 1);
        }

        @Override
        public Double getResult(Tuple2<Double, Integer> t1) {
            return t1.f0;
        }

        @Override
        public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> t1, Tuple2<Double, Integer> t2) {
            return new Tuple2<>(t1.f0 + t2.f0, t1.f1 + t2.f1);
        }
    }
}
