package src.main.sparkdemo.common;

import org.apache.spark.api.java.JavaRDD;

public interface contextProduct {

    JavaRDD makeRDD(String path);
}
