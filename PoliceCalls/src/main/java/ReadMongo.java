import com.mongodb.spark.MongoSpark;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public class ReadMongo {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("MongoSparkConnectorIntro")
                .getOrCreate();

        Dataset<Row> mongoDF = spark.read().format("mongo")
                .option("uri", "mongodb://127.0.0.1/police.callcenter")
                .load();
        mongoDF.show();
        mongoDF.printSchema();
    }
}


