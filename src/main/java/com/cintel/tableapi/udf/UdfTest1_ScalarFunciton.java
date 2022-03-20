package com.cintel.tableapi.udf;

import com.cintel.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

/**
 * 自定义scalar函数  一进一出
 * @create 2022/3/20
 * @since 1.0.0
 */
public class UdfTest1_ScalarFunciton {
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

        // 4. 自定义scalar函数，求id hash值
        IdHashCode idHashCode = new IdHashCode(21);
        //  注册udf
        tableEnv.registerFunction("idHashCode",idHashCode);


        // 4.1 table api
        Table resultTableApi = dataTable.select("id,ts,idHashCode(id)");

        // 4.2 sql
        tableEnv.createTemporaryView("sensorTable",dataTable);
        Table resultSqlTable = tableEnv.sqlQuery("select id, ts, idHashCode(id) from sensorTable");

        // 打印输出
        tableEnv.toAppendStream(resultTableApi, Row.class).print("table-api");
        tableEnv.toAppendStream(resultSqlTable, Row.class).print("sql");



        env.execute();
    }

    /**
     * 自定义scalar函数  一进一出
     *  返回数据的方法必须是eval方法，且必须是public的
     */
    public static class IdHashCode extends ScalarFunction {

        private int factor = 13;

        public IdHashCode(int factor) {
            this.factor = factor;
        }

        public int eval(String id) {
            return id.hashCode() * factor;
        }

    }
}