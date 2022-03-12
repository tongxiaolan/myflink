package com.cintel.source;

import com.cintel.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;

/**
 * 自定义数据源
 */
public class SourceUDF {
    public static void main(String[] args) throws Exception {
        // 获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(2);

        //添加自定义数据源
        DataStreamSource<SensorReading> dataStream = env.addSource(new MySensorSource());

        //打印
        dataStream.print();

        env.execute();
    }

    /**
     *  实现自定义的SourceFunction
     */
    public static class MySensorSource implements SourceFunction<SensorReading> {

        // 定义标识位
        private boolean running = true;
        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {
            // 定义随机数发生器
            Random random = new Random();

            //设置10搁传感器的初始温度
            HashMap<String,Double> sensorTempMap = new HashMap<String, Double>();
            for (int i = 0; i < 10; i++) {
                sensorTempMap.put("sensor_" + (i + 1), 60 + random.nextGaussian() * 20);
            }
            while (running) {
                for (String sensorId : sensorTempMap.keySet()) {
                    // 在当前温度基础上做随机波动
                    Double newtemp = sensorTempMap.get(sensorId) + random.nextGaussian();
                    sensorTempMap.put(sensorId, newtemp);
                    ctx.collect(new SensorReading(sensorId,System.currentTimeMillis(),newtemp));
                }
                Thread.sleep(1000);
            }
        }
        @Override
        public void cancel() {
            running = false;
        }
    }

}
