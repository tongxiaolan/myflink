package com.cintel.transform;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 分区算子
 */
public class TransFormPartitions {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStream<String> fileStream = env.readTextFile("D:\\workspace\\myflink\\src\\main\\resources\\sensors.txt");
        fileStream.print("fileStream");

        // 1 shuffle
        DataStream<String> shuffleStream = fileStream.shuffle();
        shuffleStream.print("shuffleStream");

        // 2 global
        DataStream<String> globalStream = fileStream.global();
        globalStream.print("global");
        //

        env.execute();


    }
}
