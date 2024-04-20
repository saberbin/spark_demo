package src.main.sparkdemo.application;

import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import src.main.sparkdemo.common.Application;
import src.main.sparkdemo.common.sparkEnv;
import src.main.sparkdemo.controller.sparkStreamAppController;
import src.main.sparkdemo.util.projectEnv;
import src.main.sparkdemo.util.projectSettings;

import java.io.Serializable;
import java.util.HashMap;

/**
 * @projectName: spark_demo
 * @package: src.main.sparkdemo.application
 * @className: sparkStreamSocketApplication
 * @author: NelsonWu
 * @description: spark application的示例demo， sparkstreaming的socket示例
 * @date: 2024/4/21 0:58
 * @version: 1.0
 */
public class sparkStreamSocketApplication extends Application implements Serializable {
    @Override
    public void start(String master, String appName) {
        projectEnv projEnv = new projectEnv();
        // get spark env
        // 默认配置
        projEnv.setEnv(master, appName);
        sparkEnv sparkRunEnv = projectEnv.getSparkRunEnv();
        sparkRunEnv.setSparkContext();
        // sparkRunEnv.setSparkSession();  // sparkSQL must call this function to setUp the sparkSession env
        sparkRunEnv.setStreamingContext(Durations.seconds(5));

        // project settings
        projectSettings projSettings = new projectSettings();
        projSettings.setLocalVar();
        HashMap<String, String> options = src.main.sparkdemo.util.projectSettings.getProjectSettings();
        // socket streaming options
        options.put("source", "socket");
        options.put("address", "172.29.69.38");
        options.put("port", "9999");

        try {
            sparkStreamAppController appController = new sparkStreamAppController();
            appController.dispatch();

            JavaStreamingContext streamingContext = sparkRunEnv.getStreamingContext();
            streamingContext.start();
            streamingContext.awaitTermination();
        }catch (Exception e){
            System.out.println(e);
            throw new RuntimeException(e);
        }

        projEnv.clear();
    }
    public static void main(String[] args) {
        sparkStreamSocketApplication application = new sparkStreamSocketApplication();
        application.start("local[2]", "SparkStreamingTestApp");
    }
}
