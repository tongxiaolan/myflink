package com.cintel.windows;

import com.cintel.beans.SensorReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 全窗口函数
 */
public class WindowsTest2FullWindows {
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


        DataStream<Integer> resultStream = sensorReadingStream.keyBy("id")
                .timeWindow(Time.seconds(15))
                .apply(new WindowFunction<SensorReading, Integer, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<SensorReading> input, Collector<Integer> out) throws Exception {
                        Integer count = IteratorUtils.toList(input.iterator()).size();
                        out.collect(count);
                    }
                });

        DataStream<Tuple3<String,Long,Integer>> resultStream2 = sensorReadingStream.keyBy("id")
                .timeWindow(Time.seconds(15))
                .apply(new WindowFunction<SensorReading, Tuple3<String,Long,Integer>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<SensorReading> input, Collector<Tuple3<String, Long, Integer>> out) throws Exception {
                        //总数
                        Integer count = IteratorUtils.toList(input.iterator()).size();
                        // window的结束时间
                        long end = window.getEnd();
                        //key
                        String id = tuple.getField(0);
                        out.collect(new Tuple3<>(id,end,count));
                    }
                });



//        resultStream.print("res1");
        resultStream2.print("res2");


        env.execute();
    }
}
