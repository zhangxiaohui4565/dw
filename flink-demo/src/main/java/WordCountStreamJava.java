import org.apache.flink.api.common.functions.FlatMapFunction;

import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;



public class WordCountStreamJava {

        public static void main(String[] args) throws Exception {

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


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
            env.execute("WordCountStreamJava");
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