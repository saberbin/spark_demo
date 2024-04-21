package src.main.sparkdemo.dao;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.streaming.api.java.JavaInputDStream;

import java.io.Serializable;


/**
 * @projectName: spark_demo
 * @package: src.main.sparkdemo.dao
 * @className: sparkAppDao
 * @author: NelsonWu
 * @description: 持久层，实例化3个静态方法，分别返回rdd、dataset、datastream对象
 * @date: 2024/4/17 23:18
 * @version: 1.0
 */
public class sparkAppDao implements Serializable {

    public sparkAppDao(){}

    public static JavaRDD<String> readRddData(){
        return sparkRDDAppDao.readData();
    }

    public static Dataset<Row> readDataSetData(){
        return sparkDataSetAppDao.readData();
    }

    public static JavaInputDStream readStreamData(){
        return sparkStreamAppDao.readData();
    }

}
