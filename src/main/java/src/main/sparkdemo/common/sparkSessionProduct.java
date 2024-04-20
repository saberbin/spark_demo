package src.main.sparkdemo.common;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import src.main.util.projectEnv;

import java.util.HashMap;

/**
 * @projectName: jdmo
 * @package: src.main.common
 * @className: sparkSessionProduct
 * @author: NelsonWu
 * @description: TODO
 * @date: 2024/4/19 19:23
 * @version: 1.0
 */
public class sparkSessionProduct implements sessionProduct{
    private HashMap<String, String> options = new HashMap<>();

    sparkSessionProduct(String url, String driver, String user, String passwd, String dbtable){
        this.options.put("url", url);
        this.options.put("driver", driver);
        this.options.put("user", user);
        this.options.put("password", passwd);
        this.options.put("dbtable", dbtable);
    }

    @Override
    public Dataset<Row> makeDataSet() {
        sparkEnv sparkRunEnv = projectEnv.getSparkRunEnv();
        SparkSession sparkSession = sparkRunEnv.getSparkSession();
        Dataset<Row> dataFrame = sparkSession.read().format("jdbc")
                .option("url", this.options.get("url"))
                .option("driver", this.options.get("driver"))
                .option("user", this.options.get("user"))
                .option("password", this.options.get("password"))
                .option("dbtable", this.options.get("dbtable"))
                .load();
        return dataFrame;
    }
}
