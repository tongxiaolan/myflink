package com.cintel.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 *  map,flatMap,filter的用法
 */
public class TansFormTestBase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从文件读取
        DataStream<String> inputSteam = env.readTextFile("D:\\workspace\\myflink\\src\\main\\resources\\words.txt");

        // map 把读取的字符串按长度输出  MapFunction<输入类型,输出类型>
        DataStream<Integer> mapSstream = inputSteam.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String s) throws Exception {
                return s.length();
            }
        });


        // flatmap 按空格分割
        DataStream<String> flatMapStream = inputSteam.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] fields = s.split(" ");
                for (String field : fields) {
                    collector.collect(field);
                }
            }
        });


        // filter 过滤A开头的数据
        DataStream<String> filterStream = inputSteam.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return s.startsWith("A");
            }
        });

        //打印输出
        mapSstream.print("map");
        flatMapStream.print("flatmap");
        filterStream.print("filter");

        env.execute();
    }
}
