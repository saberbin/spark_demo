package src.main.sparkdemo.service;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import src.main.sparkdemo.common.sparkEnv;
import src.main.sparkdemo.dao.sparkAppDao;
import src.main.sparkdemo.util.projectEnv;
import org.apache.spark.sql.SQLContext;
import java.io.Serializable;

/**
 * @projectName: spark_demo
 * @package: src.main.sparkdemo.service
 * @className: sparkDSAppService
 * @author: NelsonWu
 * @description: TODO
 * @date: 2024/4/20 23:09
 * @version: 1.0
 */
public class sparkDSAppService extends sparkAppService implements Serializable {
    public sparkDSAppService() {
    }

    public Dataset<Row> dataAnalysis() {
        Dataset<Row> dataset = sparkAppDao.readDataSetData();
        try {
            dataset.createTempView("area_tb");
        } catch (AnalysisException e) {
            throw new RuntimeException(e);
        }

        sparkEnv sparkRunEnv = projectEnv.getSparkRunEnv();
        SparkSession sparkSession = sparkRunEnv.getSparkSession();
        SQLContext sqlContext = sparkSession.sqlContext();

        Dataset<Row> dataset1 = sqlContext.sql("select area, city, count(1) as cnt from area_tb group by area, city order by cnt desc;");

        return dataset1;
    }
}
