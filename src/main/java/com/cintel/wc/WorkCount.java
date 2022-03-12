package com.cintel.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WorkCount {
    public static void main(String[] args) throws Exception {
        // 1 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2 从文件读取数据
        String inputPath = "D:\\workspace\\myflink\\src\\main\\resources\\words.txt";
//        DataSource<String> stringDataSource = env.readTextFile(inputPath);
        DataSet<String> ds =  env.readTextFile(inputPath);

        // 对数据集处理 空格分割，转换成元组(word,1)
        //                             按照第一个位置的word分组  按第二个位置上的数据求和
        DataSet<Tuple2<String, Integer>> resSet = ds.flatMap(new MyFlatMapper()).groupBy(0).sum(1);

        resSet.print();


    }

    // 自定义类 实现FlatMapFunction方法

    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String,Integer>> {
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            // 按空格切分
            String[] split = s.split(" ");

            // 遍历word ,包成二元组输出
            for (String word : split) {
                collector.collect(new Tuple2(word, 1))  ;
            }

        }
    }

}
