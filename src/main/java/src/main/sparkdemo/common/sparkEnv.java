package src.main.sparkdemo.common;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.Duration;
import java.io.Serializable;

/**
 * @projectName: spark_demo
 * @package: src.main.sparkdemo.common
 * @className: sparkEnv
 * @author: NelsonWu
 * @description: TODO
 * @date: 2024/4/19 0:58
 * @version: 1.0
 */
public class sparkEnv implements Serializable {
    private SparkConf sparkConf;

    private JavaSparkContext sparkContext;
    private SparkSession sparkSession;
    private JavaStreamingContext sparkStreamingContext;

    public sparkEnv(String master, String appName){
        this.sparkConf = new SparkConf().setMaster(master).setAppName(appName);
        // 如果通过streaming读取Kafka数据的时候，不配置kryo序列化，则会抛出异常
        this.sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    }

    public SparkConf getSparkConf(){return this.sparkConf;}

    public void setSparkContext(){
        this.sparkContext = new JavaSparkContext(this.sparkConf);
    }

    public JavaSparkContext getSparkContext(){
        return this.sparkContext;
    }

    public void setSparkSession(){
        this.sparkSession = SparkSession.builder().config(this.sparkConf).getOrCreate();
    }

    public SparkSession getSparkSession(){
        return this.sparkSession;
    }

    public void setStreamingContext(Duration durations){
        this.sparkStreamingContext = new JavaStreamingContext(this.sparkContext, durations);
    }

    public JavaStreamingContext getStreamingContext(){
        return this.sparkStreamingContext;
    }

    public void clear(){
        if (this.sparkSession!=null){
            this.sparkSession.stop();
        }
        if (this.sparkContext!=null){
            this.sparkContext.stop();
        }
        if (this.sparkStreamingContext!=null){
            this.sparkStreamingContext.stop();
        }

    }

}