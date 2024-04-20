package src.main.sparkdemo.application;

import src.main.sparkdemo.common.Application;
import src.main.sparkdemo.common.sparkEnv;
import src.main.sparkdemo.controller.sparkDSAppController;
import src.main.sparkdemo.util.projectEnv;
import src.main.sparkdemo.util.projectSettings;

import java.io.Serializable;
import java.util.HashMap;

/**
 * @projectName: spark_demo
 * @package: src.main.sparkdemo.application
 * @className: areaCountDSApplication
 * @author: NelsonWu
 * @description: spark application的示例demo， sparkSQL示例
 * @date: 2024/4/21 0:49
 * @version: 1.0
 */
public class areaCountDSApplication extends Application implements Serializable {

    @Override
    public void start(String master, String appName) {
        projectEnv projEnv = new projectEnv();
        // get spark env
        // 默认配置
        projEnv.setEnv(master, appName);
        sparkEnv sparkRunEnv = projectEnv.getSparkRunEnv();
        sparkRunEnv.setSparkContext();
        sparkRunEnv.setSparkSession();  // sparkSQL must call this function to setUp the sparkSession env

        // project settings
        projectSettings projSettings = new projectSettings();
        projSettings.setLocalVar();
        HashMap<String, String> options = src.main.sparkdemo.util.projectSettings.getProjectSettings();
        options.put("url", "jdbc:mysql://172.29.69.38:3306/sparktest");
        options.put("driver", "com.mysql.cj.jdbc.Driver");
        options.put("user", "root");
        options.put("password", "mysql");
        options.put("dbtable", "area_city_count");

        try {
            sparkDSAppController appController = new sparkDSAppController();
            appController.dispatch();
        }catch (Exception e){
            System.out.println(e);
            throw new RuntimeException(e);
        }

        projEnv.clear();
    }

    public static void main(String[] args) {
        areaCountDSApplication application = new areaCountDSApplication();
        application.start("local[2]", "SparkSQLTestApp");
    }
}
