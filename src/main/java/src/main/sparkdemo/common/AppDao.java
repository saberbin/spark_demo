package src.main.sparkdemo.common;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @projectName: spark_demo
 * @package: src.main.sparkdemo.common
 * @className: AppDao
 * @author: NelsonWu
 * @description: TODO
 * @date: 2024/4/18 2:13
 * @version: 1.0
 */
public abstract class AppDao implements AbstractDataReader{
    @Override
    public abstract JavaRDD readData();

}
