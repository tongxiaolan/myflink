package com.cintel.tableapi;

import com.cintel.beans.SensorReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author 小懒
 * @create 2022/3/20
 * @since 1.0.0
 */
public class TableTest5_TimeAndWindow {
    public static void main(String[] args) throws Exception {
        // 1. 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        // 2. 读入文件数据
        DataStreamSource<String> inputStream = env.readTextFile("F:\\work\\workspaces\\idea\\myflink\\src\\main\\resources\\sensors.txt");

        // 处理时间语义
        // 3. 转换成POJO
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });
        // 4. 将流转换成表，定义时间特性
        Table dataTable1 = tableEnv.fromDataStream(dataStream, "id,timestamp as ts, temperature, pt.proctime");



        // 事件事件语义
        // 3. 转换成POJO
        DataStream<SensorReading> dataStreameventTime = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(3)) {
            @Override
            public long extractTimestamp(SensorReading element) {
                return element.getTimestamp();
            }
        });

        // 4. 将流转换成表，定义时间特性
//        Table dataTable2 = tableEnv.fromDataStream(dataStreameventTime, "id,timestamp.rowtime,temperature");
        Table dataTable2 = tableEnv.fromDataStream(dataStreameventTime, "id,timestamp as ts ,temperature,rt.rowtime");


        // 5. 窗口操作
        // 5.1 group window
        // table api
        Table resultTable = dataTable2.window(Tumble.over("10.second").on("rt").as("tw"))
                .groupBy("id,tw")
                .select("id,id.count,temperature.avg,tw.end");


        // sqlapi
        tableEnv.createTemporaryView("sensor",dataTable2);                  // 窗口开始时间                       // 窗口结束时间
        Table resultSqlTable = tableEnv.sqlQuery("select id,count(id) as cnt,avg(temperature) as avgTemp,tumble_start(rt,interval '10' second),tumble_end(rt,interval '10' second)" +
                " from sensor group by id,tumble(rt,interval '10' second)");


//        tableEnv.toAppendStream(resultTable, Row.class).print("resultTable");
//        tableEnv.toRetractStream(resultSqlTable, Row.class).print("resultSqlTable");


        // 5.2 over window
        // table api
        Table overresult = dataTable2.window(Over.partitionBy("id").orderBy("rt").preceding("2.rows").as("ow"))
                .select("id,rt,id.count over ow,temperature.avg over ow ");

        // sql
        Table sqlresult = tableEnv.sqlQuery("select id, rt , count(id) over ow,avg(temperature) over ow" +
                " from sensor " +
                "window ow as (partition by id order by rt rows between 2 preceding and current row)");

        tableEnv.toAppendStream(overresult, Row.class).print("overresult");
        tableEnv.toAppendStream(sqlresult, Row.class).print("sql");



        env.execute();
    }

}