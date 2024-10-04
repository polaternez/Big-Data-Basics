import com.mongodb.spark.MongoSpark;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class Application {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\bigdata\\hadoop");

        // Add MongoDB output uri config to session
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("Police Call Service")
                .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/police.callcenter")
                .getOrCreate();

        StructType schema = new StructType()
                .add("recordId", DataTypes.IntegerType)
                .add("callDateTime", DataTypes.StringType)
                .add("priority", DataTypes.StringType)
                .add("district", DataTypes.StringType)
                .add("description", DataTypes.StringType)
                .add("callNumber", DataTypes.StringType)
                .add("incidentLocation", DataTypes.StringType)
                .add("location", DataTypes.StringType);
        Dataset<Row> policeDF = spark.read()
                .option("header", true)
                .schema(schema)
                .csv("C:\\Users\\Pantheon\\Desktop\\BigData\\Datasets\\police911.csv");

        Dataset<Row> data = policeDF.filter(
                policeDF.col("recordId").isNotNull()
        );

//        data.groupBy("incidentLocation").count().sort(functions.desc("count")).show();
        Dataset<Row> descriptionDF = data.filter(
                data.col("description").notEqual("911/NO  VOICE")
        );
        Dataset<Row> resultDF = descriptionDF.groupBy("incidentLocation", "description").count()
                .sort(functions.desc("count"));

        // Write dataset to MongoDB
        MongoSpark.write(resultDF)
                .mode("overwrite")
                .save();

    }
}
