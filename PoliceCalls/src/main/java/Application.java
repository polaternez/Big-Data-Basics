import com.mongodb.spark.MongoSpark;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class Application {
    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir", "C:\\hadoop");

        StructType schema = new StructType().add("recordId", DataTypes.IntegerType)
                .add("callDateTime", DataTypes.StringType)
                .add("priority", DataTypes.StringType)
                .add("district", DataTypes.StringType)
                .add("description", DataTypes.StringType)
                .add("callNumber", DataTypes.StringType)
                .add("incidentLocation", DataTypes.StringType)
                .add("location", DataTypes.StringType);

        SparkSession sparkSession = SparkSession.builder()
                .master("local")
                .appName("Police Call Service")
                .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/police.callcenter")
                .getOrCreate();

        Dataset<Row> rawData = sparkSession.read()
                .option("header",true)
                .schema(schema)
                .csv("C:\\Users\\Master\\Desktop\\Big Data\\Datasets\\police911.csv");

        Dataset<Row> data = rawData.filter(rawData.col("recordId").isNotNull());

//        data.groupBy("incidentLocation").count().sort(functions.desc("count")).show();
        Dataset<Row> descriptionDS = data.filter(data.col("description").notEqual("911/NO  VOICE"));
        Dataset<Row> resultDS = descriptionDS.groupBy("incidentLocation", "description").count().sort(functions.desc("count"));

        MongoSpark.write(resultDS).mode("overwrite").save();

    }
}
