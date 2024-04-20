package src.main.sparkdemo.dao;

import org.apache.spark.streaming.api.java.JavaInputDStream;
import src.main.sparkdemo.common.sparkDataProduct;
import src.main.sparkdemo.util.projectSettings;

import java.io.Serializable;
import java.util.HashMap;

/**
 * @projectName: spark_demo
 * @package: src.main.sparkdemo.dao
 * @className: sparkStreamAppDao
 * @author: NelsonWu
 * @description: 读取数据流方法实例，返回dstream对象
 * @date: 2024/4/20 22:48
 * @version: 1.0
 */
public class sparkStreamAppDao implements Serializable {
    public static JavaInputDStream readData(){
        HashMap<String, String> options = projectSettings.getProjectSettings();
        return (JavaInputDStream) sparkDataProduct.readData("streaming", options);
    }
}
