package com.zxh;

import org.apache.flink.api.common.functions.FlatMapFunction;


import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.IOException;

import static org.apache.flink.streaming.api.CheckpointingMode.EXACTLY_ONCE;


public class WordCountStreamJava {

        public static void main(String[] args) throws Exception {

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//            try {
//                env.setStateBackend(new RocksDBStateBackend(new RocksDBStateBackend("hdfs://flink01.bigdata.zxxk.com:8020/flink/checkDir", true)));
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//            env.enableCheckpointing(1200000);
//            // 保证一次性语义
//            env.getCheckpointConfig().setCheckpointingMode(EXACTLY_ONCE);
//            // 两次ck 时间不能小于50秒
//            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(50000);
//            // ck 超时时间为60秒
//            env.getCheckpointConfig().setCheckpointTimeout(60000);
//            // hdfs 上保存最近的10个状态，方便进行历史状态恢复
//            env.getCheckpointConfig().setMaxConcurrentCheckpoints(10);
//            // 5分钟内若失败了3次则认为该job失败，重试间隔为10s
//            env.setRestartStrategy(RestartStrategies.failureRateRestart(3, org.apache.flink.api.common.time.Time.seconds(60*5), org.apache.flink.api.common.time.Time.seconds(10)));
            DataStreamSource<String> sourceDstream = env.socketTextStream("10.111.118.105", 9999);


            DataStream<WordCount> wordAndOneStream = sourceDstream.flatMap(new FlatMapFunction<String, WordCount>() {
                public void flatMap(String line, Collector<WordCount> collector) throws Exception {
                    String[] words = line.split(" ");
                    for (String word : words) {
                        collector.collect(new WordCount(word, 1L));
                    }

                }
            });


            DataStream<WordCount> resultStream = wordAndOneStream
                    .keyBy("word")  //按照单词分组
                    .sum("count");   //按照count字段累加结果

            //todo: 步骤四：结果打印
            resultStream.print();

            //todo: 步骤五：任务启动
            env.execute("com.zxh.WordCountStreamJava");
        }


        public static class WordCount{
            public String word;
            public long count;
            //记得要有这个空构建
            public WordCount(){

            }
            public WordCount(String word,long count){
                this.word = word;
                this.count = count;
            }

            @Override
            public String toString() {
                return "WordCount{" +
                        "word='" + word + '\'' +
                        ", count=" + count +
                        '}';
            }

        }
    }