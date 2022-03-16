package com.cintel.tableapi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

public class TableTest3_FileOutput {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

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
                .filter(" id = 'sensor_1'");


        // 聚合统计
        Table aggTable = inputTable.groupBy("id")
                .select("id,id.count as count , temp.avg as avgTemp ");

        // 3.2 sql
        tableEnv.sqlQuery("select id, temp from inputTable where id = 'sensor_6'");
        Table sqlAggTable = tableEnv.sqlQuery("select id, count(id) as cnt,avg(temp) as avgTemp from inputTable group by id");


        // 4 输出到文件
        //链接外部文件，注册输出表
        tableEnv.connect(new FileSystem().path("D:\\workspace\\myflink\\src\\main\\resources\\out.txt"))
                .withFormat( new Csv())
                .withSchema( new Schema()
                        .field("id", DataTypes.STRING())
                        .field("temperature",DataTypes.DOUBLE())
                )
                .createTemporaryTable("outputTable");

        // 输出   注意:不支持输出聚合操作后的表到文件
        resultTable.insertInto("outputTable");

        env.execute();


    }
}
