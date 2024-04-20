package src.main.sparkdemo.service;


import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;
import src.main.sparkdemo.dao.sparkAppDao;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @projectName: spark_demo
 * @package: src.main.sparkdemo.service
 * @className: sparkAppService
 * @author: NelsonWu
 * @description: service层
 * @date: 2024/4/17 23:18
 * @version: 1.0
 */
public class sparkAppService implements Serializable {
    private sparkAppDao appDao = new sparkAppDao();

    /*
    分析，具体的处理逻辑
     */
    public JavaRDD<String> dataAnalysis(String path){
        JavaRDD<String> textRDD = appDao.readData("text", path);

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

        JavaRDD<String> result2 = resultRDD.map(new Function<Tuple2<String, Integer>, String>() {
            @Override
            public String call(Tuple2<String, Integer> value) throws Exception {
                String key = value._1();
                Integer v = value._2();
                return key + "->" + v;
            }
        });

        return result2;

    }

}
