package src.main.sparkdemo.controller;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import src.main.sparkdemo.common.Controller;
import src.main.sparkdemo.service.sparkAppService;
import org.apache.spark.api.java.JavaRDD;
import src.main.sparkdemo.util.projectEnv;
import java.io.Serializable;

/**
 * @projectName: spark_demo
 * @package: src.main.sparkdemo.controller
 * @className: sparkAppController
 * @author: NelsonWu
 * @description: 控制层
 * @date: 2024/4/17 23:18
 * @version: 1.0
 */
public class sparkAppController extends Controller implements Serializable {
    private sparkAppService appService = new sparkAppService();

    /*
    调度
     */
    public void dispatch(String path){

        JavaRDD<String> rdd = appService.dataAnalysis(path);
        rdd.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
    }

}
