package com.cintel.processfunction;

import com.cintel.beans.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 测试KeyedProcessFunction
 */
public class ProcessTest1_KeyedProcessFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputStream = env.socketTextStream("192.168.128.131", 6666);

        // 转换成pojo
        DataStream<SensorReading> sensorReadingStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });

        //测试keyedProcessFunction,先分组然后自定义处理
        sensorReadingStream.keyBy("id")
                .process(new MyKeyedProcessFunction())
                .print();

        env.execute();

    }

    /**
     * 自定义processFunction来处理数据
     */
    public static class MyKeyedProcessFunction extends KeyedProcessFunction<Tuple,SensorReading,Integer> {

        ValueState<Long> tsTimerState;
        @Override
        public void open(Configuration parameters) throws Exception {
            tsTimerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts-timer",Long.class));
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<Integer> out) throws Exception {
            out.collect(value.getId().length());

            // 针对当前key的操作


            //context
            ctx.timestamp();// 当前数据时间戳
            ctx.getCurrentKey();//当前key
//            ctx.output();// 分流 输出到测输出流
            ctx.timerService().currentProcessingTime(); // 获取当前处理时间
            ctx.timerService().currentWatermark();// 获取当前watermark
            ctx.timerService().registerEventTimeTimer(value.getTimestamp()+100000L); // 注册事件时间的定时器
            ctx.timerService().registerProcessingTimeTimer(value.getTimestamp()+100000L); //注册处理时间的定时器

            tsTimerState.update(value.getTimestamp());// 记录注册定时器时所用的时间戳，用来取消定时器

            ctx.timerService().deleteEventTimeTimer(tsTimerState.value()+10000L); //取消定时器
            ctx.timerService().deleteProcessingTimeTimer(tsTimerState.value()+10000L);//取消定时器
        }

        // 定时器到时间 以后触发的操作
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Integer> out) throws Exception {
            System.out.println(timestamp + "定时器触发");
            ctx.getCurrentKey();
//            ctx.output();    侧输出流
            // ctx.timeDomain() 时间域，用来查看当前是什么时间语义的定时器
        }
    }
}
