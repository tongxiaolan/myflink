package com.cintel.processfunction;


import com.cintel.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class ProcessTest3_SideOutputCase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputStream = env.socketTextStream("192.168.128.131", 6666);

        // 转换成pojo
        DataStream<SensorReading> sensorReadingStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });

        //  定义测输出流tag
        OutputTag<SensorReading> lowTemp = new OutputTag<SensorReading>("lowTemp"){};

        //测试keyedProcessFunction, 自定义测输出流实现分流操作
        SingleOutputStreamOperator<SensorReading> highTempStream = sensorReadingStream.process(new ProcessFunction<SensorReading, SensorReading>() {
            @Override
            public void processElement(SensorReading sensorReading, Context context, Collector<SensorReading> collector) throws Exception {
                //判断温度大于30 输出到主流，如果低于30 输出到测流
                if(sensorReading.getTemperature()>30) {
                    collector.collect(sensorReading);
                }else {
                    context.output(lowTemp,sensorReading);
                }
            }
        });



        highTempStream.print("high");
        highTempStream.getSideOutput(lowTemp).print("low");

        env.execute();
    }
}
