package com.cintel.wc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamWorkCount {

    public static void main(String[] args) throws Exception {
        // 1 创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(8);


        // 2从文件中读取数据
        //String inputPath = "D:\\workspace\\myflink\\src\\main\\resources\\words.txt";
        //DataStream<String> stream = env.readTextFile(inputPath);

        // 用 parameter tool 工从启动参数中提取配置  启动参数中  添加 --host 192.168.128.132 --port 6666
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");

        // 读取sopket文本数据流   nc -lk 6666   然后输入
//        DataStream<String> stream = env.socketTextStream("192.168.128.132", 6666);
        DataStream<String> stream = env.socketTextStream(host, port);



        // 基于数据流进行转换计算
        DataStream<Tuple2<String, Integer>> sum = stream.flatMap(new WorkCount.MyFlatMapper()).keyBy(0).sum(1);

        sum.print();
        // 启动任务
        env.execute();

    }

}
