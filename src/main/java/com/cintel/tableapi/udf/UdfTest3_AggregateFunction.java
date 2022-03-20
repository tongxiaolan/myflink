package com.cintel.tableapi.udf;

import com.cintel.beans.SensorReading;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.types.Row;

/**
 * 自定义聚合函数  多进一出
 * @create 2022/3/20
 * @since 1.0.0
 */
public class UdfTest3_AggregateFunction {
    public static void main(String[] args) throws Exception {
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

        Table dataTable = tableEnv.fromDataStream(dataStream, "id,timestamp as ts,temperature");

        // 4. 自定义聚合函数，求当前传感器的平均温度值
        AvgTemp avgTemp = new AvgTemp();
        //  注册udf
        tableEnv.registerFunction("avgTemp", avgTemp);

        // 4.1 table api
        Table resultTableApi = dataTable.groupBy("id")
                .aggregate("avgTemp(temperature) as avg_temp")
                .select("id, avg_temp");

        // 4.2 sql
        tableEnv.createTemporaryView("sensorTable", dataTable);
        Table resultSqlTable = tableEnv.sqlQuery("select id, avgTemp(temperature) from sensorTable group by id");

        // 打印输出  有更新 所以要用 toRetractStream
        tableEnv.toRetractStream(resultTableApi, Row.class).print("table-api");
        tableEnv.toRetractStream(resultSqlTable, Row.class).print("sql");


        env.execute();
    }


    /**
     * 自定义聚合函数  多进一出
     * AggregateFunction 第一个参数：返回值  第二个参数：中间状态
     */
    public static class AvgTemp extends AggregateFunction<Double, Tuple2<Double, Integer>> {
        @Override
        public Double getValue(Tuple2<Double, Integer> accumulator) {
            return accumulator.f0 / accumulator.f1;
        }

        // 创建累加器
        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0.0, 0);
        }

        // 必须实现一个accumulate方法 来数据之后更新数据 第一个参数：累加的值，第二个：新的值
        public void accumulate(Tuple2<Double, Integer> accumulator, Double temp) {
            accumulator.f0 += temp;
            accumulator.f1++;
        }


    }
}