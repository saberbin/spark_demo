package src.main.sparkdemo.application;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import src.main.sparkdemo.common.Application;
import src.main.sparkdemo.common.sparkEnv;
import src.main.sparkdemo.controller.sparkAppController;
import src.main.sparkdemo.util.projectEnv;
import java.io.Serializable;

/**
 * @projectName: spark_demo
 * @package: src.main.sparkdemo.application
 * @className: sparkApplication
 * @author: NelsonWu
 * @description: TODO
 * @date: 2024/4/17 23:17
 * @version: 1.0
 */
public class sparkApplication extends Application implements Serializable {

    @Override
    public void start(String master, String appName) {
        super.start();

        projectEnv projectEnv = new projectEnv();
        // get spark env
        // 默认配置
        projectEnv.setEnv("local[2]", "sparkApp");

        sparkEnv sparkRunEnv = projectEnv.getSparkRunEnv();

        sparkRunEnv.setSparkContext();

        try {
            sparkAppController appController = new sparkAppController();
            appController.dispatch("/home/saberbin/data/wc.txt");
        }catch (Exception e){
            System.out.println(e);
        }

        projectEnv.clear();
    }

    public static void main(String[] args) {

        sparkApplication sparkApplication = new sparkApplication();
        sparkApplication.start("local[*]", "sparkProjApp");

    }
}
