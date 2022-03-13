package com.cintel.state;

import com.cintel.beans.SensorReading;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author 小懒
 * @create 2022/3/13
 * @since 1.0.0
 */
public class StateTest3_KeyedStateApplicationCase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> inputStream = env.socketTextStream("192.168.128.131", 6666);

        // 转换成pojo
        DataStream<SensorReading> sensorReadingStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });

        // 定义一个flatmap操作，检测温度跳变，输出警报
        SingleOutputStreamOperator<Tuple3<String, Double, Double>> resultStream = sensorReadingStream.keyBy("id")
                .flatMap(new TempChangeWaring(10.0));


        resultStream.print();
        env.execute();
    }


    //实现自定义函数类
    public static class TempChangeWaring extends RichFlatMapFunction<SensorReading, Tuple3<String, Double, Double>> {

        private Double threshold;

        public TempChangeWaring(Double threshold) {
            this.threshold = threshold;
        }

        // 定义状态，保存上一次的温度值
        private ValueState<Double> lastTempState;

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp", Double.class));
        }

        @Override
        public void flatMap(SensorReading sensorReading, Collector<Tuple3<String, Double, Double>> collector) throws Exception {
            // 获取上一次的温度值
            Double lastTemp = lastTempState.value();

            //判断如果不为null 那么就对比温度差值
            if (lastTemp != null) {
                Double diff = Math.abs(sensorReading.getTemperature() - lastTemp);
                if (diff >= threshold) {
                    collector.collect(new Tuple3<>(sensorReading.getId(), lastTemp, sensorReading.getTemperature()));
                }
            }

            // 更新状态
            lastTempState.update(sensorReading.getTemperature());
        }

        @Override
        public void close() throws Exception {
            lastTempState.clear();
        }
    }


}