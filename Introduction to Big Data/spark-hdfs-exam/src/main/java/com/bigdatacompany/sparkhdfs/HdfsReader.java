package com.bigdatacompany.sparkhdfs;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HdfsReader {
    public static void main(String[] args) {
        
        System.setProperty("hadoop.home.dir","C:\\hadoop");

        SparkSession sparkSession = SparkSession.builder().master("local").appName("Hdfs Reader").getOrCreate();

        Dataset<Row> rawData = sparkSession.read().option("header", true).csv("hdfs://localhost:8020/data/movies.csv");

        Dataset<Row> millenniumDS = rawData.filter(rawData.col("title").contains("(2000)"));

        millenniumDS.show();
        millenniumDS.write().csv("hdfs://localhost:8020/data/millennium");
//        millenniumDS.coalesce(1).write().partitionBy("genres").csv("hdfs://localhost:8020/data/millennium");

    }
}
