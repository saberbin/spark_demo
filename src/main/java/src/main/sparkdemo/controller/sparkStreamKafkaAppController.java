package src.main.sparkdemo.controller;

import org.apache.spark.streaming.api.java.JavaDStream;
import src.main.sparkdemo.common.Controller;
import src.main.sparkdemo.service.sparkStreamKafkaAppService;

import java.io.Serializable;

/**
 * @projectName: spark_demo
 * @package: src.main.sparkdemo.controller
 * @className: sparkStreamKafkaAppController
 * @author: NelsonWu
 * @description: dispatch方法与streamAppController一致，但是sparkStreamKafkaAppService的dataAnalysis方法处理kafka的ConsumerRecord对象
 * @date: 2024/4/21 1:35
 * @version: 1.0
 */
public class sparkStreamKafkaAppController  extends Controller implements Serializable {
    private sparkStreamKafkaAppService appService = new sparkStreamKafkaAppService();

    @Override
    public void dispatch() {
        JavaDStream<String> outputStream = appService.dataAnalysis();
        outputStream.print();

    }
}
