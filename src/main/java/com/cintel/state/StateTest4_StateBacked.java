package com.cintel.state;

import com.cintel.beans.SensorReading;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author 小懒
 * @create 2022/3/13
 * @since 1.0.0
 */
public class StateTest4_StateBacked {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //状态后端配置
        env.setStateBackend(new MemoryStateBackend());
        env.setStateBackend(new FsStateBackend("hdfs:///flink/checkpoint"));
        env.setStateBackend(new RocksDBStateBackend(""));

        // 配置检查点  默认500ms
        env.enableCheckpointing(300);

        // 高级选项

        // checkpoint mode (精准一次，至少一次)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        // 同时执行的个数  前一个checkpoint可能还没完， 后一个可能已经触发了   默认是1
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);

        // 一个checkpoint完成以后， 最小间隔这个时间以后才会执行下一次checkpoint ,该配置可以覆盖配置的检查点间隔时间
        // 每300ms checkpoint一次，如果这次用时250ms，下次会在50ms以后执行，但是有了这个配置，必须在100ms以后才能执行
        // 如果用时执行checkpoint执行用时150ms,剩余的时间超过了100ms,那么正常执行，下次在150ms以后
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(100L);

        // 如果设置为true,恢复的时候会从最近的检查点恢复，而不是保存点
        // 默认是false 恢复的时候不管是保存点还是检查点，从最近的一个去恢复
        env.getCheckpointConfig().setPreferCheckpointForRecovery(true);

        // 默认0 可以容忍checkpoint失败几次以后，认为任务失败
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);

        // 重启策略
        // 回滚重启 基于平台的   例如yarn的重启机制
        env.setRestartStrategy(RestartStrategies.fallBackRestart());
        // 不重启
        env.setRestartStrategy(RestartStrategies.noRestart());
        // 固定延迟重启 每个10s重启一次，重启3次总共
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,10000L));
        // 在10分钟之内,每次重启间隔1分钟，只尝试3次                      重试次数       时间段内         重试间隔
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.minutes(10),Time.minutes(1)));




        DataStream<String> inputStream = env.socketTextStream("192.168.128.131", 6666);

        // 转换成pojo
        DataStream<SensorReading> sensorReadingStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });

        sensorReadingStream.print();

        env.execute();
    }

}