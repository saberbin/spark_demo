package src.main.sparkdemo.service;


import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;
import src.main.sparkdemo.dao.sparkAppDao;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;


/**
 * @projectName: spark_demo
 * @package: src.main.sparkdemo.service
 * @className: sparkAppService
 * @author: NelsonWu
 * @description: service层，负责具体的分析、逻辑处理
 * @date: 2024/4/17 23:18
 * @version: 1.0
 */
public class sparkAppService implements Serializable {
    private sparkAppDao appDao = new sparkAppDao();

    public sparkAppDao getAppDao(){
        return this.appDao;
    }

    public Object dataAnalysis(){
        return null;
    }

}
