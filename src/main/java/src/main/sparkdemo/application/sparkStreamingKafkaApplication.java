package src.main.sparkdemo.application;

import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import src.main.sparkdemo.common.Application;
import src.main.sparkdemo.common.sparkEnv;
import src.main.sparkdemo.controller.sparkStreamKafkaAppController;
import src.main.sparkdemo.util.projectEnv;
import src.main.sparkdemo.util.projectSettings;

import java.io.Serializable;
import java.util.HashMap;

/**
 * @projectName: spark_demo
 * @package: src.main.sparkdemo.application
 * @className: sparkStreamingKafkaApplication
 * @author: NelsonWu
 * @description: spark application的示例demo， sparkstreaming的Kafka示例
 * @date: 2024/4/21 1:13
 * @version: 1.0
 */
public class sparkStreamingKafkaApplication extends Application implements Serializable {
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
        // streaming kafka settings
        options.put("source", "kafka");
        options.put("server", "172.29.69.38:9092");
        options.put("groupid", "test");
        options.put("offset", "latest");
        options.put("topic", "test");

        try {
            sparkStreamKafkaAppController appController = new sparkStreamKafkaAppController();
            appController.dispatch();

            JavaStreamingContext streamingContext = sparkRunEnv.getStreamingContext();
            streamingContext.start();
            streamingContext.awaitTermination();
        }catch (Exception e){
            throw new RuntimeException(e);
        }

        projEnv.clear();
    }
    public static void main(String[] args) {
        sparkStreamingKafkaApplication application = new sparkStreamingKafkaApplication();
        application.start("local[2]", "SparkStreamingTestApp");
    }
}
