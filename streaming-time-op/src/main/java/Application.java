import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class Application {
    public static void main(String[] args) throws StreamingQueryException {

        System.setProperty("hadoop.home.dir","C:\\hadoop");

        SparkSession sparkSession = SparkSession.builder().master("local").appName("streaming-time-op").getOrCreate();

        Dataset<Row> raw_data = sparkSession.readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", "8000")
                .option("includeTimestamp", true).load();

        Dataset<Row> products = raw_data.as(Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP())).toDF("product", "timestamp");

        Dataset<Row> resultData = products.groupBy(
                functions.window(products.col("timestamp"), "1 minute"),
                products.col("product")
        ).count().orderBy("window");

        StreamingQuery start = resultData.writeStream()
                .outputMode("complete")
                .format("console")
                .option("truncate", false)
                .start();

        start.awaitTermination();


    }
}
