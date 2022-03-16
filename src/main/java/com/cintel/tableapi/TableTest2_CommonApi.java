package com.cintel.tableapi;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 *
 */
public class TableTest2_CommonApi {
    public static void main(String[] args) throws Exception {
        // 1 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1.1 获取老版本的planner的流处理
        EnvironmentSettings oldStreamSettings = EnvironmentSettings.newInstance()
                .useOldPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment oldStreamTableEnv = StreamTableEnvironment.create(env, oldStreamSettings);


        // 1.2 基于老版本的planner的批处理
        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment oldBatchTableEnv = BatchTableEnvironment.create(batchEnv);


        // 1.3 基于blink 的流处理
        EnvironmentSettings blinkStreamSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment blinkStreamTableEnv = StreamTableEnvironment.create(env, blinkStreamSettings);


        // 1.4 基于blink的批处理
        EnvironmentSettings blinkBatchSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inBatchMode()
                .build();
        TableEnvironment blinkBatchTableEnv = TableEnvironment.create(blinkBatchSettings);


        // 2. 表的创建：链接外部系统，读取数据
        // 2.1 读取文件
        String filePath = "D:\\workspace\\myflink\\src\\main\\resources\\sensors.txt";

        // 注册表并规范字段等信息
        tableEnv.connect(new FileSystem().path(filePath))
                .withFormat( new Csv())
                .withSchema( new Schema()
                    .field("id", DataTypes.STRING())
                    .field("timestamp",DataTypes.BIGINT())
                    .field("temp",DataTypes.DOUBLE())
                )
                .createTemporaryTable("inputTable");

        // 读取表
        Table inputTable = tableEnv.from("inputTable");

        // 3. 查询转换
        // 3.1 table api
        // 简单转换
        Table resultTable = inputTable.select("id,temp")
                .filter(" id = 'sensor_1'");// .filter(" id === 'sensor_1'")  2种写法结果一样


        // 聚合统计
        Table aggTable = inputTable.groupBy("id")
                .select("id,id.count as count , temp.avg as avgTemp ");

        // 3.2 sql
        tableEnv.sqlQuery("select id, temp from inputTable where id = 'sensor_6'");
        Table sqlAggTable = tableEnv.sqlQuery("select id, count(id) as cnt,avg(temp) as avgTemp from inputTable group by id");


        // 打印表信息
        // 2的打印输出   toAppendStream 转换普通查出来的数据，一条一条追加的  toRetractStream 转换聚合操作的 撤回之前一条数据，输出新数据
        inputTable.printSchema();
        tableEnv.toAppendStream(inputTable, Row.class).print("inputTable");

        // 3.2打印输出
        tableEnv.toAppendStream(resultTable,Row.class).print("resultTable");
        tableEnv.toRetractStream(aggTable,Row.class).print("aggTable");
        tableEnv.toRetractStream(sqlAggTable,Row.class).print("sqlAggTable");


        env.execute();

    }
}
