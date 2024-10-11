package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

// Press Shift twice to open the Search Everywhere dialog and type `show whitespaces`,
// then press Enter. You can now see whitespace characters in your code.
public class WriterApp {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\bigdata\\hadoop");

        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("Istanbul Sites")
                .getOrCreate();

        StructType locSchema = new StructType()
                .add("lat", DataTypes.DoubleType)
                .add("lon", DataTypes.DoubleType);
        StructType schema = new StructType()
                .add("location", locSchema)
                .add("stitle", DataTypes.StringType);
        Dataset<Row> siteDF = spark.read()
                .option("multiline", true)
                .schema(schema)
                .json("C:\\Users\\Pantheon\\Desktop\\BigData\\Datasets\\Applications\\GeogspatialQuery\\istanbul_siteler.json");

        // JavaEsSpark from elasticsearch-spark-20_2.12
        JavaEsSpark.saveJsonToEs(siteDF.toJSON().toJavaRDD(), "sites/_doc");
    }
}