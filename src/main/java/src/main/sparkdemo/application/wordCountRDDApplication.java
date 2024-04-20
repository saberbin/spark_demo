package src.main.sparkdemo.application;

import src.main.sparkdemo.common.Application;
import src.main.sparkdemo.common.sparkEnv;
import src.main.sparkdemo.controller.sparkRDDAppController;
import src.main.sparkdemo.util.projectEnv;
import src.main.sparkdemo.util.projectSettings;

import java.io.Serializable;
import java.util.HashMap;

/**
 * @projectName: spark_demo
 * @package: src.main.sparkdemo.application
 * @className: wordCountRDDApplication
 * @author: NelsonWu
 * @description: word count application for test，spark application的示例demo
 * @date: 2024/4/21 0:34
 * @version: 1.0
 */
public class wordCountRDDApplication extends Application implements Serializable {

    @Override
    public void start(String master, String appName) {

        projectEnv projEnv = new projectEnv();
        // get spark env
        // 默认配置
        projEnv.setEnv(master, appName);
        sparkEnv sparkRunEnv = projectEnv.getSparkRunEnv();
        sparkRunEnv.setSparkContext();

        // project settings
        projectSettings projSettings = new projectSettings();
        projSettings.setLocalVar();
        HashMap<String, String> options = src.main.sparkdemo.util.projectSettings.getProjectSettings();
        options.put("path", "/home/saberbin/data/wc.txt");

        try {
            sparkRDDAppController appController = new sparkRDDAppController();
            appController.dispatch();
        }catch (Exception e){
            System.out.println(e);
        }

        projEnv.clear();
    }

    public static void main(String[] args) {
        wordCountRDDApplication application = new wordCountRDDApplication();
        application.start("local[2]", "wordCountApp");
    }

}
