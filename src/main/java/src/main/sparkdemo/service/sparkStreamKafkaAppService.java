package src.main.sparkdemo.service;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;
import src.main.sparkdemo.dao.sparkAppDao;


import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

/**
 * @projectName: spark_demo
 * @package: src.main.sparkdemo.service
 * @className: sparkStreamKafkaAppService
 * @author: NelsonWu
 * @description: TODO
 * @date: 2024/4/21 1:26
 * @version: 1.0
 */
public class sparkStreamKafkaAppService extends sparkAppService implements Serializable {
    public sparkStreamKafkaAppService() {
    }

    public JavaDStream dataAnalysis(){
        JavaInputDStream<ConsumerRecord<String, String>> inputDStream = sparkAppDao.readStreamData();

        JavaPairDStream<String, Integer> kvInputStream = inputDStream.flatMap(new FlatMapFunction<ConsumerRecord<String,String>, String>() {
            @Override
            public Iterator<String> call(ConsumerRecord<String, String> record) throws Exception {
                return Arrays.asList(record.value().split(" ")).iterator();
            }
        }).mapToPair(x -> new Tuple2<>(x, 1));

        JavaPairDStream<String, Integer> result = kvInputStream.reduceByKey((x, y) -> x + y);

        return result.map(x -> x._1() + "->" + x._2());
    }
}
