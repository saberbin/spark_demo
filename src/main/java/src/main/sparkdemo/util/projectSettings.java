package src.main.sparkdemo.util;

import java.util.HashMap;
import java.io.Serializable;

/**
 * @projectName: spark_demo
 * @package: src.main.sparkdemo.util
 * @className: projectSettings
 * @author: NelsonWu
 * @description: 项目配置共享变量
 * @date: 2024/4/20 21:57
 * @version: 1.0
 */
public class projectSettings implements Serializable {
    private static ThreadLocal<HashMap<String, String>> localVar = new ThreadLocal<HashMap<String, String>>();

    public void setLocalVar(){
        projectSettings.localVar.set(new HashMap<String, String>());
    }

    public static HashMap<String, String> getProjectSettings(){
        return projectSettings.localVar.get();
    }

    public void clear(){
        this.setLocalVar();
    }

}
