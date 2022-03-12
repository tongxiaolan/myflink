package com.cintel.transform;

import com.cintel.beans.SensorReading;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;


/**
 * @author 小懒
 * @create 2022/3/9
 * @since 1.0.0
 */
public class SinkJdbc {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> inputStream = env.readTextFile("F:\\work\\workspaces\\idea\\wp3\\myflink\\src\\main\\resources\\words.txt");

        // 转换成pojo
        DataStream<SensorReading> sensorReadingStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });

        sensorReadingStream.addSink(new MyJdbcSink());

        env.execute();
    }


     /**
     * 自定义 jdbc sink
     */
    public static class MyJdbcSink extends RichSinkFunction<SensorReading>{
        // 声明链接和预编译语句
        Connection connection = null;
        PreparedStatement insertStmt = null;
        PreparedStatement updateStmt = null;


        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection("jdbc://localhost:3306/test","root","123456");
            // 创建预编译器，有占位符，可传入参数
            insertStmt = connection.prepareStatement("INSERT INTO sensor_temp (id, temp) VALUES (?, ?)");
            updateStmt = connection.prepareStatement("UPDATE sensor_temp SET temp = ? WHERE id  = ?");

        }

        // 没来一条数据，调用链接，执行sql
        @Override
        public void invoke(SensorReading value, Context context) throws Exception {
            // 执行更新语句，注意不要留 super
            updateStmt.setDouble(1, value.getTemperature());
            updateStmt.setString(2, value.getId());
            updateStmt.execute();
            // 如果刚才 update 语句没有更新，那么插入
            if (updateStmt.getUpdateCount() == 0) {
                insertStmt.setString(1, value.getId());
                insertStmt.setDouble(2, value.getTemperature());
                insertStmt.execute();
            }

        }

        @Override
        public void close() throws Exception {
            insertStmt.close();
            updateStmt.close();
            connection.close();
        }
    }

}