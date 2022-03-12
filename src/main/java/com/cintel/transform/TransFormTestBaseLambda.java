package com.cintel.transform;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransFormTestBaseLambda {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> inputStream = env.readTextFile("D:\\workspace\\myflink\\src\\main\\resources\\words.txt");

        //2种写法 复杂就加括号
        DataStream<Integer> mapStream = inputStream.map(line -> line.length());
        DataStream<Integer> mapStream1 = inputStream.map(line -> {
            return line.length();
        });


        //2种写法 复杂就加括号
        DataStream<String> filterStream = inputStream.filter(line -> {
            return  line.startsWith("A");
        });
        DataStream<String> filterStream1 = inputStream.filter(line -> line.startsWith("A"));

        mapStream.print("map");
        filterStream.print("filter");
        env.execute();
    }
}
