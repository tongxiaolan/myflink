package com.cintel.tableapi;

import com.cintel.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class TableTest1_Example {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 读取数据
        DataStreamSource<String> inputStream = env.readTextFile("D:\\workspace\\myflink\\src\\main\\resources\\sensors.txt");

        // 2. 转换成POJO
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });

        dataStream.print("dataStream");

        // 3. 创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 4. 基于流创建一张表
        Table dataTable = tableEnv.fromDataStream(dataStream);

        // 5. 调用table api 进行操作
        Table resultTable = dataTable.select("id,temperature")
                .where("id = 'sonser_1'");

        // 6. 执行sql  先注册
        tableEnv.createTemporaryView("sensor",dataTable);
        String sql = "select id,temperature from sensor where id = 'sonser_1'";
        Table resultSqlTable = tableEnv.sqlQuery(sql);

        //输出结果
        tableEnv.toAppendStream(resultTable, Row.class).print("resultTable");
        tableEnv.toAppendStream(resultSqlTable, Row.class).print("resultSqlTable");



        env.execute();

    }
}
