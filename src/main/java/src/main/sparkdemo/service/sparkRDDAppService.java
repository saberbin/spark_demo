package src.main.sparkdemo.service;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import src.main.sparkdemo.dao.sparkAppDao;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

/**
 * @projectName: spark_demo
 * @package: src.main.sparkdemo.service
 * @className: sparkRDDAppService
 * @author: NelsonWu
 * @description: java spark rdd word count application service demo
 * @date: 2024/4/20 21:39
 * @version: 1.0
 */
public class sparkRDDAppService extends sparkAppService implements Serializable {

    public sparkRDDAppService() {
    }

    public JavaRDD<String> dataAnalysis(){
        JavaRDD<String> textRDD = sparkAppDao.readRddData();
        JavaRDD<String> mapRDD = textRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        JavaPairRDD<String, Integer> mapRDD2 = mapRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> resultRDD = mapRDD2.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        return resultRDD.map(x -> x._1() + "->" + x._2());

    }

}
