package com.cintel.tableapi.udf;

import com.cintel.beans.SensorReading;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * 自定义table 函数 一进多出
 * @create 2022/3/20
 * @since 1.0.0
 */
public class UdfTest2_TableFunction {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1. 读取数据
        DataStreamSource<String> inputStream = env.readTextFile("F:\\work\\workspaces\\idea\\myflink\\src\\main\\resources\\sensors.txt");

        // 2. 转换成POJO
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });



        // 3. 转换成table

        Table dataTable = tableEnv.fromDataStream(dataStream,"id,timestamp as ts,temperature");

        // 4. 自定义表函数，将id拆分，并输出(word,length)
        Split split = new Split("_");
        //  注册udf
        tableEnv.registerFunction("split",split);

        // 4.1 table api
        Table resultTableApi = dataTable.joinLateral("split(id) as (word,length)")
                .select("id,ts,word,length");

        // 4.2 sql
        tableEnv.createTemporaryView("sensorTable",dataTable);
        Table resultSqlTable = tableEnv.sqlQuery("select id, ts, word,length from sensorTable ,lateral table(split(id)) as splitid(word,length)"  );

        // 打印输出  每条2条结果, sonser_1 用'_'切分
        tableEnv.toAppendStream(resultTableApi, Row.class).print("table-api");
        tableEnv.toAppendStream(resultSqlTable, Row.class).print("sql");



        env.execute();
    }


    /**
     * 自定义table 函数 一进多出
     * 泛型为返回值的类型
     * 必须实现eval方法，没有返回值
     */
    public static class Split extends TableFunction<Tuple2<String, Integer>> {
        // 定义属性(分隔符)
        private String separator=",";

        public Split(String separator) {
            this.separator = separator;
        }


        public void eval(String str){
            for (String s : str.split(separator)) {
                // 输出结果
                collect(new Tuple2<>(s, s.length()));
            }
        }
    }


}