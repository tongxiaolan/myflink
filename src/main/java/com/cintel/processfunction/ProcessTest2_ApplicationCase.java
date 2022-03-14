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
 * 检测连续10s温度上升的数据
 * 采用 processFunction  数据来了以后，开一个10s的定时器，将开窗的时间和当前的温度值记录在状态中
 * 如果10s中来了温度降低的，取消定时器，如果没有来，则输出报警信息
 * 问题:只来了一条数据/来了连续上升的几条数据但是后续没有再来数据的，都会报警
 */
public class ProcessTest2_ApplicationCase {
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
                .process(new TempConsIncreWaring(10))
                .print();

        env.execute();

    }


    public static class TempConsIncreWaring extends KeyedProcessFunction<Tuple, SensorReading, String> {

        // 定义私有属性，当前统计的时间间隔
        private Integer interval;

        public TempConsIncreWaring(Integer interval) {
            this.interval = interval;
        }

        // 定义状态 保存上一次的温度值，定时器时间戳
        private ValueState<Double> lastTempState;
        private ValueState<Long> timerTsState;


        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("lastTempState", Double.class, Double.MIN_VALUE));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerTsState", Long.class));
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
            // 取出状态
            Double lastTemp = lastTempState.value();
            Long timerTs = timerTsState.value();

            // 如果温度上升并且没有定时器，注册10s定时器，开始等待
            if (value.getTemperature() > lastTemp && timerTs == null) {
                //计算出定时器时间戳
                Long ts = ctx.timerService().currentProcessingTime() + interval * 1000L;
                ctx.timerService().registerProcessingTimeTimer(ts);
                timerTsState.update(ts);
            }
            // 如果温度下降，删除定时器
            else if (value.getTemperature() < lastTemp && timerTs != null) {
                ctx.timerService().deleteProcessingTimeTimer(timerTs);
                timerTsState.clear();
            }

            // 更新温度状态
            lastTempState.update(value.getTemperature());
        }


        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发 输出报警信息
            out.collect("传感器" + ctx.getCurrentKey().getField(0) + "温度连续" + interval + "上升");
            timerTsState.clear();
        }


        @Override
        public void close() throws Exception {
            lastTempState.clear();
            timerTsState.clear();
        }
    }

}
