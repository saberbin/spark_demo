package src.main.sparkdemo.controller;

import src.main.sparkdemo.common.Controller;
import src.main.sparkdemo.service.sparkAppService;

import java.io.Serializable;

/**
 * @projectName: spark_demo
 * @package: src.main.sparkdemo.controller
 * @className: sparkAppController
 * @author: NelsonWu
 * @description: 控制层，调度作用。具体的需求需要继承该类实现dispatch方法
 * @date: 2024/4/17 23:18
 * @version: 1.0
 */
public class sparkAppController extends Controller implements Serializable {
    private sparkAppService appService = new sparkAppService();

    public void dispatch(){

    }

}
