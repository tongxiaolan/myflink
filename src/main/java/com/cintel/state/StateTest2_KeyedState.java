package com.cintel.state;

import com.cintel.beans.SensorReading;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * @author 小懒
 *
 * 键控状态
 *
 * @create 2022/3/13
 * @since 1.0.0
 */
public class StateTest2_KeyedState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> inputStream = env.socketTextStream("192.168.128.131", 6666);

        // 转换成pojo
        DataStream<SensorReading> sensorReadingStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });

        // 定义一个有状态的map操作，统计当前sensor个数
        sensorReadingStream.keyBy("id")
                .map(new MyKeyCountMapper());


       env.execute();
    }

    //自定义mapfunction
    public static class MyKeyCountMapper extends RichMapFunction<SensorReading,Integer> {

        private ValueState<Integer> keyCountState;

        // 其他类型状态声明
        private ListState<String> myListState;
        private MapState<String,Double> myMapState;
        private ReducingState<SensorReading> myReduceingState;


        // getRuntimeCountext需要在环境准备好以后才能调用 ，所以不能直接定义的时候调用赋值
        @Override
        public void open(Configuration parameters) throws Exception {
            keyCountState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("key-count", Integer.class));

            myListState = getRuntimeContext().getListState(new ListStateDescriptor<String>("my-list", String.class));

            myMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Double>("my-map", String.class, Double.class));

//            myReduceingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<SensorReading>("my-reduceing",new MyKeyCountMapper(),SensorReading.class));
        }

        @Override
        public Integer map(SensorReading sensorReading) throws Exception {
            // list state
            // 也需要先判空
            if(myListState.get() == null) {}
            for (String str : myListState.get()) {
                System.out.println(str);
            }
            List<String> list = new ArrayList<>();
            list.add("1");
            list.add("2");
            myListState.add("aaa");     //添加单个元素
            myListState.update(list);   //更新
            myListState.addAll(list);   //添加整个集合


            // map state 常规的map操作有
            myMapState.get("1");
            myMapState.put("2", 33.22);


            //  reduceing state  定义聚合方法，调用add方法的时候会执行自定义的聚合方法
            myReduceingState.add(sensorReading);


            Integer count = keyCountState.value();
            if(count==null) {
                count = 0;
            }
            count++;
            keyCountState.update(count);
            return count;
        }


        @Override
        public void close() throws Exception {
            keyCountState.clear();
            myListState.clear();
            myMapState.clear();
            myReduceingState.clear();
        }
    }
}