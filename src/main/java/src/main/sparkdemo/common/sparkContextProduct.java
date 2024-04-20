package src.main.sparkdemo.common;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import src.main.util.projectEnv;

/**
 * @projectName: jdmo
 * @package: src.main.common
 * @className: sparkContextProduct
 * @author: NelsonWu
 * @description: TODO
 * @date: 2024/4/19 16:41
 * @version: 1.0
 */
public class sparkContextProduct implements contextProduct{
    sparkContextProduct(){}
    @Override
    public JavaRDD makeRDD(String path) {
        JavaSparkContext sparkContext = projectEnv.getSparkRunEnv().getSparkContext();
        return sparkContext.textFile(path);
    }

}
