package src.main.sparkdemo.controller;

import org.apache.spark.streaming.api.java.JavaDStream;
import src.main.sparkdemo.common.Controller;
import src.main.sparkdemo.service.sparkStreamAppService;
import java.io.Serializable;

/**
 * @projectName: spark_demo
 * @package: src.main.sparkdemo.controller
 * @className: sparkStreamAppController
 * @author: NelsonWu
 * @description: 持久层，具体的stream调度方法，打印输出
 * @date: 2024/4/21 0:26
 * @version: 1.0
 */
public class sparkStreamAppController extends Controller implements Serializable {
    private sparkStreamAppService appService = new sparkStreamAppService();

    @Override
    public void dispatch() {
        JavaDStream<String> outputStream = appService.dataAnalysis();
        outputStream.print();

    }
}
