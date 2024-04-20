package src.main.sparkdemo.common;


import org.apache.spark.api.java.JavaRDD;

import java.util.HashMap;


/**
 * @projectName: jdmo
 * @package: src.main.common
 * @className: sparkDataProduct
 * @author: NelsonWu
 * @description: TODO
 * @date: 2024/4/19 19:41
 * @version: 1.0
 */
public class sparkDataProduct {

    public static Object readData(String typeName, HashMap<String, String> options){
        String typeNameLower = typeName.toLowerCase();
        if (typeNameLower.equals("textfile")){
            sparkContextProduct sparkContextProduct = new sparkContextProduct();
            JavaRDD<String> rdd = sparkContextProduct.makeRDD(options.get("path"));
            return rdd;
        } else if (typeNameLower.equals("jdbc")) {
            sparkSessionProduct sparkSessionProduct = new sparkSessionProduct(
                    options.get("url"),
                    options.get("driver"),
                    options.get("user"),
                    options.get("password"),
                    options.get("dbtable")
            );
            return sparkSessionProduct.makeDataSet();
        } else if (typeNameLower.equals("streaming")) {
            sparkStreamingProduct streamingProduct = new sparkStreamingProduct(
                    options.get("source"),
                    options
            );
            return streamingProduct.makeDStream();
        }else {
            throw new RuntimeException("typeName must in: textfile, jdbc, streaming.");
        }

    }
}
