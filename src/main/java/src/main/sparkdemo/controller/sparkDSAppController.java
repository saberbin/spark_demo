package src.main.sparkdemo.controller;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import src.main.sparkdemo.common.Controller;
import src.main.sparkdemo.service.sparkDSAppService;
import java.io.Serializable;

/**
 * @projectName: spark_demo
 * @package: src.main.sparkdemo.controller
 * @className: sparkDSAppController
 * @author: NelsonWu
 * @description: 持久层，具体的dataset调度方法，打印输出
 * @date: 2024/4/21 0:22
 * @version: 1.0
 */
public class sparkDSAppController extends Controller implements Serializable {
    private sparkDSAppService appService = new sparkDSAppService();


    @Override
    public void dispatch() {
        Dataset<Row> dataset = appService.dataAnalysis();
        dataset.show();
    }
}
