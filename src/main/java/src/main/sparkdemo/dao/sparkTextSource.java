package src.main.sparkdemo.dao;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import src.main.sparkdemo.common.AppDao;
import src.main.sparkdemo.common.sparkEnv;
import src.main.sparkdemo.util.projectEnv;
import java.io.Serializable;

/**
 * @projectName: spark_demo
 * @package: src.main.sparkdemo.dao
 * @className: SparkTextSource
 * @author: NelsonWu
 * @description: TODO
 * @date: 2024/4/18 15:01
 * @version: 1.0
 */
public class sparkTextSource extends AppDao implements Serializable {

    private String textFilePath;

    sparkTextSource(String path){
        this.textFilePath = path;
    }

    @Override
    public JavaRDD<String> readData() {
        sparkEnv sparkRunEnv = projectEnv.getSparkRunEnv();
        JavaSparkContext sparkContext = sparkRunEnv.getSparkContext();
        return sparkContext.textFile(this.textFilePath);
    }
}
