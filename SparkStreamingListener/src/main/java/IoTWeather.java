import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;


public class IoTWeather {
    public static void main(String[] args) throws StreamingQueryException {
        System.setProperty("hadoop.home.dir", "C:\\bigdata\\hadoop");

        SparkSession sparkSession = SparkSession.builder().master("local").appName("SparkStreamingMessageListener").getOrCreate();
        StructType weatherType = new StructType()
                .add("quarter", DataTypes.StringType)
                .add("heatType", DataTypes.StringType)
                .add("heat", DataTypes.IntegerType)
                .add("windType", DataTypes.StringType)
                .add("wind", DataTypes.IntegerType);

        Dataset<Row> rawData = sparkSession.readStream()
                .schema(weatherType)
                .option("sep", ",")
                .csv("C:\\Users\\Polat\\Desktop\\BigData\\Datasets\\sparkstreaming\\*");

        Dataset<Row> heatData = rawData.select("quarter", "heat", "wind")
                .where("heat>29 AND wind>29");

        StreamingQuery query = heatData.writeStream()
                .format("console")
                .start();

        query.awaitTermination();

    }
}