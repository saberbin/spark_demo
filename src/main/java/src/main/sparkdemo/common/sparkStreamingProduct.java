package src.main.sparkdemo.common;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import src.main.util.projectEnv;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * @projectName: jdmo
 * @package: src.main.common
 * @className: sparkStreamingProduct
 * @author: NelsonWu
 * @description: TODO
 * @date: 2024/4/19 21:13
 * @version: 1.0
 */
public class sparkStreamingProduct implements streamingProduct, Serializable {
    private String sourceName;
    private HashMap<String, String> options = new HashMap<>();
    private HashMap<String, Object> kafkaConfig = new HashMap<String, Object>();

    sparkStreamingProduct(String source, HashMap<String, String> options){
        this.sourceName = source;
        if (source.equals("socket")){
            this.options.put("address", options.get("address"));
            this.options.put("port", options.get("port"));

        } else if (source.equals("kafka")) {
            sparkEnv sparkRunEnv = projectEnv.getSparkRunEnv();
            this.kafkaConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, options.get("server"));
            this.kafkaConfig.put(ConsumerConfig.GROUP_ID_CONFIG, options.get("groupid"));
            this.kafkaConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            this.kafkaConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            this.kafkaConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_DOC, options.get("offset"));
            this.kafkaConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);  // 默认非自动提交
            this.options.put("topic", options.get("topic"));
        }
    }

    @Override
    public JavaInputDStream makeDStream() {
        sparkEnv sparkRunEnv = projectEnv.getSparkRunEnv();
        JavaStreamingContext streamingContext = sparkRunEnv.getStreamingContext();
        if (this.sourceName.equals("socket")){
            return streamingContext.socketTextStream(this.options.get("address"), Integer.valueOf(this.options.get("port")));
        } else if (this.sourceName.equals("kafka")) {
            List<String> topic = Arrays.asList(this.options.get("topic"));  // topic列表
            JavaInputDStream<ConsumerRecord<String, String>> kafkaStream = KafkaUtils.createDirectStream(
                    streamingContext,  // spark streaming context实例
                    LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.<String, String>Subscribe(
                            topic,  // 需要传入collection对象
                            this.kafkaConfig  // 需要传入map对象
                    )
            );
            return kafkaStream;

        }

        return null;
    }
}
