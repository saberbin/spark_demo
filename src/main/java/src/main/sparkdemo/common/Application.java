package src.main.sparkdemo.common;

import java.io.Serializable;

/**
 * @projectName: spark_demo
 * @package: src.main.sparkdemo.common
 * @className: Application
 * @author: NelsonWu
 * @description: TODO
 * @date: 2024/4/17 23:42
 * @version: 1.0
 */
public abstract class Application implements TApplication, Serializable {
    @Override
    public void start() {

    }

    public abstract void start(String master, String appName);
}
