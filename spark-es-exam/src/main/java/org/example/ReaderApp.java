package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ReaderApp {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\bigdata\\hadoop");

        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("Es Spark SQL Reader")
                .getOrCreate();

        Dataset<Row> loadDF = spark.read().format("org.elasticsearch.spark.sql")
                .option("es.nodes", "localhost")
                .option("es.port", "9200")
                .load("sites");
        loadDF.show();
        loadDF.printSchema();
    }
}
