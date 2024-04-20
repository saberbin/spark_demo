package src.main.sparkdemo.dao;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;


/**
 * @projectName: spark_demo
 * @package: src.main.sparkdemo.dao
 * @className: sparkAppDao
 * @author: NelsonWu
 * @description: 持久层
 * @date: 2024/4/17 23:18
 * @version: 1.0
 */
public class sparkAppDao implements Serializable {

    public sparkAppDao(){}

    public static JavaRDD<String> readData(String dataType, String path){

        if (dataType.equals("text")){
            sparkTextSource sparkTextSource = new sparkTextSource(path);
            return sparkTextSource.readData();
        }else {
            return null;
        }

    }

}
