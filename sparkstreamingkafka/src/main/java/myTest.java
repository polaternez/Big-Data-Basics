import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

public class myTest {
    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        System.setProperty("hadoop.home.dir", "C:\\bigdata\\hadoop");

        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("streaming-kafka")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        StructType schema = new StructType()
                .add("product", DataTypes.StringType)
                .add("time", DataTypes.StringType);
        Dataset<Row> kafkaDF = spark.readStream().format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "search")
                .load();

        Dataset<Row> extractedDF = kafkaDF.selectExpr("CAST(value AS STRING)")
                .select(functions.from_json(functions.col("value"), schema).as("data"))
                .select("data.*");
//        extractedDF.show();

       /* Dataset<Row> countDF = extractedDF.groupBy(functions.window(extractedDF.col("time"), "1 minute"), extractedDF.col("product")).count();
        countDF.show();*/

        Dataset<Row> countDF = extractedDF.groupBy("product").count();

        StreamingQuery query = countDF.writeStream()
                .outputMode("update")
                .format("console")
                .start();
        query.awaitTermination();
    }
}
