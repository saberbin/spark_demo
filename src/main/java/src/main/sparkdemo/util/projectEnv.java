package src.main.sparkdemo.util;

import src.main.sparkdemo.common.sparkEnv;

import java.io.Serializable;

/**
 * @projectName: spark_demo
 * @package: src.main.sparkdemo.util
 * @className: sparkEnv
 * @author: NelsonWu
 * @description: TODO
 * @date: 2024/4/19 0:51
 * @version: 1.0
 */
public class projectEnv implements Serializable {
    private static ThreadLocal<sparkEnv> localVar = new ThreadLocal<sparkEnv>();

    public void setEnv(String master, String appName){
        projectEnv.localVar.set(new sparkEnv(master, appName));
    }

    public static sparkEnv getSparkRunEnv(){
        return projectEnv.localVar.get();
    }

    public void clear(){
        if (projectEnv.localVar!=null){
            localVar.get().clear();
        }
    }

}
