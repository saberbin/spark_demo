# spark_demo

spark工程，依据Java项目常见三层架构模式，分为
- controller：控制层
- service：服务层
- dao：持久层
- application：应用程序层
- common：通用类抽象方法
- util：工具类
- bean：实体类

在此基础上做了一定程度的封装以及常见案例的demo。

## 主要目录说明
### common
通用类抽象方法。但是此处除了controller、service、application的底层接口及抽象类之外，还有dao层的数据读取接口，由于dao层的底层数据读取做了抽象工厂的方法，在命名上与其他层的抽象方法有所区别，在此处进行说明。

application的抽象接口及抽象类比较简单，只是定义了程序入口的抽象方法：
```java
// 抽象接口
public interface TApplication {

    void start();
}
// 抽象类，继承TApplication接口
public abstract class Application implements TApplication, Serializable {
    @Override
    public void start() {

    }

    public abstract void start(String master, String appName);
}
```

controller层与上面一致
```java
public interface TController {

    void dispatch();

}
public abstract class Controller implements TController{
    @Override
    public abstract void dispatch();
}
```

dao层原本也是与上面的定义方式一致，但是无法满足我最终的需求，所以就采用了抽象工厂的方式。
我的需求是该项目实现spark-core、spark-sql、spark-streaming的通用工厂项目，可以根据实际需求选择对应的spark实现方式，故dao层采用了抽象工厂的方式实现了这三种数据读取方式。

rdd方面是定义了`contextProduct`的接口，通过`sparkContextProduct`继承该接口实现具体的读取方法：
```java
public interface contextProduct {

    JavaRDD makeRDD(String path);
}

public class sparkContextProduct implements contextProduct{
    sparkContextProduct(){}
    @Override
    public JavaRDD makeRDD(String path) {
        JavaSparkContext sparkContext = projectEnv.getSparkRunEnv().getSparkContext();
        // 此处只读取了textfile的方法，可以继承该接口实现其他的方式读取数据
        return sparkContext.textFile(path);
    }

}
```
dataset方面也是一样的实现方式，只是实现了`JDBC`的读取数据方式：
```java
public interface sessionProduct {
    Dataset makeDataSet();
}
public class sparkSessionProduct implements sessionProduct{
    private HashMap<String, String> options = new HashMap<>();

    sparkSessionProduct(String url, String driver, String user, String passwd, String dbtable){
        this.options.put("url", url);
        this.options.put("driver", driver);
        this.options.put("user", user);
        this.options.put("password", passwd);
        this.options.put("dbtable", dbtable);
    }

    @Override
    public Dataset<Row> makeDataSet() {
        sparkEnv sparkRunEnv = projectEnv.getSparkRunEnv();
        SparkSession sparkSession = sparkRunEnv.getSparkSession();
        Dataset<Row> dataFrame = sparkSession.read().format("jdbc")
                .option("url", this.options.get("url"))
                .option("driver", this.options.get("driver"))
                .option("user", this.options.get("user"))
                .option("password", this.options.get("password"))
                .option("dbtable", this.options.get("dbtable"))
                .load();
        return dataFrame;
    }
}
```

streaming也是一致，此处实现了`socket`以及`kafka`的读取方式
```java
public interface streamingProduct {
    JavaInputDStream makeDStream();
}
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
```

除此之外，common目录中还有当前项目的环境对象，包含了`spark-core`、`spark-sql`、`spark-streaming`的上下文环境对象的构建方法。
```java
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
```
在实例化的时候会创建`SparkConf`对象实例，然后根据需求再创建对应的上下文对象，如果在调用上下文对象的时候没有在application层配置，则会抛出空指针异常。
该对象实例会存放到`projectEnv`的thread local变量中供其他层调用。

### dao
dao层主要是读取数据，具体通过3个子类去实现，
```text
sparkRDDAppDao
sparkDataSetAppDao
sparkStreamAppDao
```
`sparkAppDao`封装了3个方法，分别调用3个子类的具体方法去读取数据。


### service
service层的基类`sparkAppService`提供了一个实现模板，其余子类分别对应了各种具体的实现
```text
sparkRDDAppService
sparkDSAppService
sparkStreamAppService
sparkStreamKafkaAppService
```

### controller
controller层主要是与service层进行交互，`dispatch`方法调用具体的service层的`dataAnalysis`
```java
public class sparkAppService implements Serializable {
    private sparkAppDao appDao = new sparkAppDao();

    public sparkAppDao getAppDao(){
        return this.appDao;
    }

    public Object dataAnalysis(){
        return null;
    }

}
```

### util
util目录下主要有两个关键的类：`projectEnv`、`projectSettings`
`projectEnv`主要是共享spark的上下文环境，`projectSettings`主要是共享项目内全局的配置，供读取数据时调用。
这两者都要在`application层进行设置（主要也是为了减少配置的地方以及数量，而增加的类）。


### application
application是具体的项目主程序入口，基础的demo
```java
public class sparkApplication extends Application implements Serializable {

    @Override
    public void start(String master, String appName) {

    }

    public static void main(String[] args) {
    }
}
```
如果需要实现，建议以此demo为基础。
application目录内提供了常见的spark具体案例的实现，包括spark-core、spark-sql、spark-streaming的案例：
```text
wordCountRDDApplication是基于rdd的word count案例
areaCountDSApplication是基于spark-sql的dataset的统计案例，通过jdbc连接MySQL数据读取数据库统计数据
sparkStreamSocketApplication是基于socket的streaming的word count案例
sparkStreamingKafkaApplication是基于Kafka数据源的streaming的word count案例
```
可以基于上述的demo实现具体的案例。

## 环境配置
```text
java 1.8+
spark 3.0+
mysql 8.0
kafka 3.4.0
```

## maven环境
```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>src.main</groupId>
    <artifactId>spark_demo</artifactId>
    <version>1.0-SNAPSHOT</version>

    <properties>
        <maven.compiler.source>8</maven.compiler.source>
        <maven.compiler.target>8</maven.compiler.target>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <spark.version>3.3.2</spark.version>

    </properties>

    <dependencies>
        <dependency> <!-- Spark dependency -->
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-core_2.12</artifactId>
            <version>3.3.2</version>
            <scope>provided</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-streaming_2.12</artifactId>
            <version>3.3.2</version>
            <scope>provided</scope>
        </dependency>
        <!-- https://mvnrepository.com/artifact/org.apache.spark/spark-sql -->

        <dependency>
            <groupId>mysql</groupId>
            <artifactId>mysql-connector-java</artifactId>
            <version>8.0.27</version>
            <scope>provided</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-streaming-kafka-0-10_2.12</artifactId>
            <version>3.3.2</version>
            <scope>provided</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql_2.12</artifactId>
            <version>3.3.2</version>
            <scope>provided</scope>
        </dependency>
        <dependency>
            <groupId>com.fasterxml.jackson.core</groupId>
            <artifactId>jackson-core</artifactId>
            <version>2.12.7</version>
            <scope>provided</scope>
        </dependency>
        <!-- http://repo1.maven.org/maven2/com/alibaba/druid/ -->
        <dependency>
            <groupId>com.alibaba</groupId>
            <artifactId>druid</artifactId>
            <version>1.1.12</version>
        </dependency>
        <!--        <dependency>-->
        <!--            <groupId>log4j</groupId>-->
        <!--            <artifactId>log4j</artifactId>-->
        <!--            <version>1.2.17</version>-->
        <!--        </dependency>-->
        <!--        <dependency>-->
        <!--            <groupId>org.slf4j</groupId>-->
        <!--            <artifactId>slf4j-api</artifactId>-->
        <!--            <version>1.7.30</version>-->
        <!--        </dependency>-->
        <!--        <dependency>-->
        <!--            <groupId>org.slf4j</groupId>-->
        <!--            <artifactId>slf4j-log4j12</artifactId>-->
        <!--            <version>1.7.7</version>-->
        <!--        </dependency>-->
    </dependencies>

</project>
```

