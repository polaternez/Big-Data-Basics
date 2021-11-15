import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class myTest {
    public static void main(String[] args) throws StreamingQueryException {

        System.setProperty("hadoop.home.dir", "C:\\hadoop");

        SparkSession sparkSession = SparkSession.builder().master("local").appName("streaming-kafka").getOrCreate();

        StructType schema = new StructType()
                .add("product", DataTypes.StringType)
                .add("time", DataTypes.StringType);

        Dataset<Row> loadDS = sparkSession.readStream().format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "search")
                .load();

        Dataset<Row> rawDS = loadDS.selectExpr("CAST(value AS STRING)")
                .select(functions.from_json(functions.col("value"), schema).as("data"))
                .select("data.*");

//        rawDS.show();

       /* Dataset<Row> countDS = rawDS.groupBy(functions.window(rawDS.col("time"), "1 minute"), rawDS.col("product")).count();

        countDS.show();*/

        Dataset<Row> countDS = rawDS.groupBy("product").count();
        
        StreamingQuery start = countDS.writeStream()
                .outputMode("update")
                .format("console")
                .start();

        start.awaitTermination();
    }
}
