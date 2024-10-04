package com.bigdatacompany.spark;

import com.google.gson.JsonObject;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.functions.*;

import java.util.ArrayList;

import static org.apache.spark.sql.functions.*;

public class MyTest {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\bigdata\\hadoop");

        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("My Test")
                .getOrCreate();
        Dataset<Row> df = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("C:\\Users\\Pantheon\\Desktop\\BigData\\Datasets\\user_test.csv");
        df.show(false);
        df.printSchema();

        // -- Shape of dataset --
//        System.out.println(String.format("Shape: (%d, %d)", df.count(), df.columns().length));

        // -- Summary statistics --
//        df.summary().show();

        // -- Check null values --
        /*ArrayList<String> nullValueCounts = new ArrayList<>();
        for(var col : df.columns()){
            nullValueCounts.add(
                    String.format("%-12s : %d", col, df.select(col).filter(df.col(col).isNull()).count())
            );
        }
        System.out.println("Null values:");
        for (var count : nullValueCounts) {
            System.out.println(count);
        }*/

        // --Drop null rows--
        Dataset<Row> cleanedDF = df.na().drop();

        // --Drop columns--
        /*Dataset<Row> newDF = cleanedDF.drop("full_name", "email");
        newDF.show();*/

        // --Add new column--
       /* cleanedDF.withColumn("age_group",
                when(col("age").lt(20), "<20")
                        .when(col("age").lt(40), "20-39")
                        .when(col("age").lt(60), "40-59")
                        .otherwise(">59")).show();*/

        // --agg method--
        /*Dataset<Row> aggDF = cleanedDF.groupBy("country").agg(
                count("id").as("count"),
                avg("age").as("avg_age"),
                sum("salary").as("sum_salary")
        );
        aggDF.sort(functions.desc("sum_salary")).show();*/

        // -- Pivot table--
//        cleanedDF.groupBy("country").pivot("gender").count().show();
        /*cleanedDF.groupBy("country")
                .pivot("gender").avg("age")
                .na().fill(0)
                .show();*/


        // --Join two datasets--
        /*Dataset<Row> bookDF = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("C:\\Users\\Pantheon\\Desktop\\BigData\\Datasets\\books.csv");
        bookDF.show(5);

        Dataset<Row> ratingsDF = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("C:\\Users\\Pantheon\\Desktop\\BigData\\Datasets\\ratings.csv");
        ratingsDF.show(5);

        bookDF.join(ratingsDF, "book_id")
                .show(5);*/
    }
}
