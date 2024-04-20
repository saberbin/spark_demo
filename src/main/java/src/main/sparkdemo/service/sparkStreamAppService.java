package src.main.sparkdemo.service;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;
import src.main.sparkdemo.dao.sparkAppDao;


/**
 * @projectName: spark_demo
 * @package: src.main.sparkdemo.service
 * @className: sparkStreamAppService
 * @author: NelsonWu
 * @description: TODO
 * @date: 2024/4/20 23:58
 * @version: 1.0
 */
public class sparkStreamAppService extends sparkAppService implements Serializable {
    public sparkStreamAppService() {
    }

    public JavaDStream dataAnalysis(){
        JavaInputDStream<String> inputDStream = sparkAppDao.readStreamData();

        JavaPairDStream<String, Integer> kvInputStream = inputDStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        }).mapToPair(x -> new Tuple2<>(x, 1));

        JavaPairDStream<String, Integer> result = kvInputStream.reduceByKey((x, y) -> x + y);

        return result.map(x -> x._1() + "->" + x._2());
    }

}
