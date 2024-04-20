package src.main.sparkdemo.controller;

import org.apache.spark.api.java.JavaRDD;
import src.main.sparkdemo.common.Controller;
import src.main.sparkdemo.service.sparkRDDAppService;

import java.io.Serializable;

/**
 * @projectName: spark_demo
 * @package: src.main.sparkdemo.controller
 * @className: sparkRDDAppController
 * @author: NelsonWu
 * @description: 持久层，具体的rdd调度方法，打印输出
 * @date: 2024/4/21 0:11
 * @version: 1.0
 */
public class sparkRDDAppController extends Controller implements Serializable {
    private sparkRDDAppService appService = new sparkRDDAppService();

    @Override
    public void dispatch(){
        JavaRDD<String> rdd = appService.dataAnalysis();
        rdd.foreach(x -> System.out.println(x));
    }
}
