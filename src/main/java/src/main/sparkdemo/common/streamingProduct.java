package src.main.sparkdemo.common;

import org.apache.spark.streaming.api.java.JavaInputDStream;

public interface streamingProduct {
    JavaInputDStream makeDStream();
}
