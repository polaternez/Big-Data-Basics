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

        SparkSession sparkSession = SparkSession.builder().master("local").appName("My Test").getOrCreate();
        Dataset<Row> rawDS = sparkSession.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("C:\\Users\\Polat\\Desktop\\BigData\\Datasets\\user_test.csv");

        rawDS.show(false);
        rawDS.printSchema();

        // -- Shape of dataset --
//        System.out.println(String.format("(%d, %d)", rawDS.count(), rawDS.columns().length));

        // -- Summary statistics --
//        rawDS.summary().show();

        // -- Find null values --
       /* ArrayList<String> nullValueCounts = new ArrayList<>();

        for(var col : rawDS.columns()){
            nullValueCounts.add(col + " : " + rawDS.select(col).filter(rawDS.col(col).isNull()).count());
        }

        System.out.println("Null values:");
        for (var count : nullValueCounts) {
            System.out.println(count);
        }*/

        // --Drop null rows--
        Dataset<Row> userDS = rawDS.na().drop();

        // --Drop columns--
        /*Dataset<Row> newDS = userDS.drop("full_name", "email");
        newDS.show();*/


        // --Add column--
       /* userDS.withColumn("age_group",
                when(col("age").lt(20), "<20")
                        .when(col("age").lt(40), "20-39")
                        .when(col("age").lt(60), "40-59")
                        .otherwise(">59")).show();*/


        // --agg method--
        /*Dataset<Row> aggDS = userDS.groupBy("country").agg(
                count("id").as("count"),
                avg("age").as("avg_age"),
                sum("salary").as("sum_salary"));

        aggDS.sort(functions.desc("sum_salary")).show();*/

        // -- Pivot table--
/*//        userDS.groupBy("country").pivot("gender").count().na().fill(0).show();
        userDS.groupBy("country").pivot("gender").avg("age").na().fill(0).show();*/


        // --Join two datasets--
    /*    Dataset<Row> bookDS = sparkSession.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("C:\\Users\\Polat\\Desktop\\BigData\\Datasets\\books.csv");

        bookDS.show(5);

        Dataset<Row> ratingsDS = sparkSession.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("C:\\Users\\Polat\\Desktop\\BigData\\Datasets\\ratings.csv");

        ratingsDS.show(5);

        bookDS.join(ratingsDS, "book_id").show(5);*/
    }
}
