package src.main.sparkdemo.common;

import org.apache.spark.api.java.JavaRDD;

public interface AbstractDataReader {
    JavaRDD readData();
}
