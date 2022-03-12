package com.cintel.transform;

import com.cintel.beans.SensorReading;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author 小懒
 * @create 2022/3/9
 * @since 1.0.0
 */
public class SinkElasticSearch {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> inputStream = env.readTextFile("F:\\work\\workspaces\\idea\\wp3\\myflink\\src\\main\\resources\\words.txt");

        // 转换成pojo
        DataStream<SensorReading> sensorReadingStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });


        // 定义es的链接配置
        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("192.168.128.131", 9200));

        // 添加es sink
        sensorReadingStream.addSink(new ElasticsearchSink.Builder<SensorReading>(httpHosts,new MyEsSinkFunction()).build());

        env.execute();

    }

    /**
     * 实现es写入操作
     */
    public static  class MyEsSinkFunction implements ElasticsearchSinkFunction<SensorReading>{

        // sensorReading 要写入的数据   runtimeContext 上下文    requestIndexer 用这个写入es
        @Override
        public void process(SensorReading sensorReading, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
            // 定义写入es的source
            HashMap<String, String> dataSource = new HashMap<>();
            dataSource.put("id", sensorReading.getId());
            dataSource.put("temp", sensorReading.getTemperature().toString());
            dataSource.put("ts", sensorReading.getTimestamp().toString());

            // 创建请求，作为向es发起的写入命令
            IndexRequest request = Requests.indexRequest()
                    .index("sensor")
                    .type("string")
                    .source(dataSource);

            // 用requestIndexer发送请求
            requestIndexer.add(request);
        }
    }
}