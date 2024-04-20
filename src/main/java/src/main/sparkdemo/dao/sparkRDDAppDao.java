package src.main.sparkdemo.dao;

import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;
import java.util.HashMap;

import src.main.sparkdemo.common.sparkDataProduct;
import src.main.sparkdemo.util.projectSettings;

/**
 * @projectName: spark_demo
 * @package: src.main.sparkdemo.dao
 * @className: sparkRDDAppDao
 * @author: NelsonWu
 * @description: 读取数据具体实例，返回rdd对象
 * @date: 2024/4/20 22:06
 * @version: 1.0
 */
public class sparkRDDAppDao implements Serializable {

    public static JavaRDD<String> readData(){
        // must insure that path in options obj or throw a Exception.
        HashMap<String, String> options = projectSettings.getProjectSettings();
        // read text file product a javaRDD data obj
        return (JavaRDD<String>) sparkDataProduct.readData("textfile", options);
    }

}
