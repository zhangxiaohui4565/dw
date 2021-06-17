package com.zxh;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.io.IOException;
import java.util.Properties;

/**
 * @author zhangxh
 * @version 1.0
 * @date 2021/4/19 11:51
 */
public class KafkaDemo {
    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka01.bigdata.zxxk.com:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "demo1");
//        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>("xyio_tracklog", new SimpleStringSchema(), properties);
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<String>("demo",
                (KafkaSerializationSchema<String>) new SimpleStringSchema(),
                properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        try {
//            env.setStateBackend(new RocksDBStateBackend(new RocksDBStateBackend("hdfs://flink01.bigdata.zxxk.com:8020/flink/checkDir", true)));
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        env.enableCheckpointing(1200000);
//        // 两次ck 时间不能小于50秒
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(50000);
//        // ck 超时时间为60秒
//        env.getCheckpointConfig().setCheckpointTimeout(60000);
//        // hdfs 上保存最近的10个状态，方便进行历史状态恢复
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(10);
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, org.apache.flink.api.common.time.Time.seconds(60 * 5), org.apache.flink.api.common.time.Time.seconds(10)));

        DataStreamSource<String> stringDataStreamSource = env.addSource(consumer);
        stringDataStreamSource.filter(new FilterFunction<String>() {
            public boolean filter(String a) throws Exception {
                return !StringUtils.isNullOrWhitespaceOnly(a);
            }
        }).addSink(producer);
        env.execute("zxh_demo");
    }
}
