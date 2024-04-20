package src.main.sparkdemo.dao;

import java.io.Serializable;
import java.util.HashMap;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import src.main.sparkdemo.common.sparkDataProduct;
import src.main.sparkdemo.util.projectSettings;

/**
 * @projectName: spark_demo
 * @package: src.main.sparkdemo.dao
 * @className: sparkSessionAppDao
 * @author: NelsonWu
 * @description: 读取数据方法， 返回dataset对象
 * @date: 2024/4/20 22:45
 * @version: 1.0
 */
public class sparkDataSetAppDao implements Serializable {
    public static Dataset<Row> readData(){
        HashMap<String, String> options = projectSettings.getProjectSettings();
        return (Dataset<Row>) sparkDataProduct.readData("jdbc", options);
    }
}
