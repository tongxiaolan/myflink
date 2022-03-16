package com.cintel.tableapi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

public class TableTest4_KafkaPipeline {
    public static void main(String[] args) throws Exception {
        // 1. 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. 链接kafka,读取数据
        tableEnv.connect( new Kafka()
                .version("0.11")
                .topic("sensor")
                .property("zookeeper.connect","localhost:2181")
                .property("bootstrap.servers","localhost:9092")
        )
                .withFormat(new Csv())
                .withSchema( new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamp",DataTypes.BIGINT())
                        .field("temp",DataTypes.DOUBLE())
                )
                .createTemporaryTable("inputTable");

        // 3. 查询转换
        // 简单转换
        Table sensorTable = tableEnv.from("inputTable");
        Table resultTable = sensorTable.select("id,temp")
                .filter(" id = 'sensor_1'");


        // 聚合统计
        Table aggTable = sensorTable.groupBy("id")
                .select("id,id.count as count , temp.avg as avgTemp ");

        // 4. 建立kafka链接 输出到不同的topic下
        tableEnv.connect( new Kafka()
                .version("0.11")
                .topic("sensorSinkTest")
                .property("zookeeper.connect","localhost:2181")
                .property("bootstrap.servers","localhost:9092")
        )
                .withFormat(new Csv())
                .withSchema( new Schema()
                        .field("id", DataTypes.STRING())
//                        .field("timestamp",DataTypes.BIGINT())
                        .field("temp",DataTypes.DOUBLE())
                )
                .createTemporaryTable("outputTable");

        // 5. 输出结果 聚合操作后的结果也不可以写入，因为有更新操作，kafka是消息队列，不支持更新操作
        resultTable.insertInto("outputTable");



        env.execute();
    }
}
