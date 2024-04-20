package src.main.sparkdemo.dao;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import src.main.sparkdemo.common.AppDao;
import src.main.sparkdemo.common.sparkEnv;
import src.main.sparkdemo.util.projectEnv;
import java.io.Serializable;
import java.util.HashMap;

/**
 * @projectName: spark_demo
 * @package: src.main.sparkdemo.dao
 * @className: sparkJDBCSource
 * @author: NelsonWu
 * @description: TODO
 * @date: 2024/4/19 14:24
 * @version: 1.0
 */
public class sparkJDBCSource extends AppDao implements Serializable {
    private HashMap<String, String> options;

    sparkJDBCSource(String url, String driver, String user, String passwd, String dbtable){
        this.options.put("url", url);
        this.options.put("driver", driver);
        this.options.put("user", user);
        this.options.put("password", passwd);
        this.options.put("dbtable", dbtable);
    }


    @Override
    public JavaRDD readData() {
        sparkEnv sparkRunEnv = projectEnv.getSparkRunEnv();
        SparkSession sparkSession = sparkRunEnv.getSparkSession();
        Dataset<Row> dataFrame = sparkSession.read()
                .option("url", this.options.get("url"))
                .option("driver", this.options.get("driver"))
                .option("user", this.options.get("user"))
                .option("password", this.options.get("password"))
                .option("dbtable", this.options.get("dbtable"))
                .load();
        return null;
    }
}
