package com.bigdatacompany.spark;

import com.google.gson.JsonObject;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.functions.*;

import java.util.ArrayList;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.count;

public class MyTest {
    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir", "C:\\hadoop");

        SparkSession sparkSession = SparkSession.builder().master("local").appName("My Test").getOrCreate();

        Dataset<Row> rawDS = sparkSession.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("C:\\Users\\Master\\Desktop\\Big Data\\Datasets\\user_test.csv");

        rawDS.show(10);
//        rawDS.printSchema();

        // Null value counts
        ArrayList<String> nullValueCounts = new ArrayList<>();

        for(var colName : rawDS.columns()){
            nullValueCounts.add(colName + " : " + rawDS.select(colName).filter(rawDS.col(colName).isNull()).count());
        }

        for (var count : nullValueCounts) {
            System.out.println(count);
        }

        // -- pivot--
    //  rawDS.groupBy("country").pivot("gender").count().na().fill(0).show();


        // --agg--
        /*Dataset<Row> aggDS = rawDS.groupBy("gender").agg(
                avg("age").as("avg_age"),
                avg("salary").as("avg_salary"));
        aggDS.show();*/

       // --drop--
     /*   Dataset<Row> newDS = rawDS.drop("full_name", "email");
        newDS.show();*/


        // --join--
      /*  Dataset<Row> bookDS = sparkSession.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("C:\\Users\\Master\\Desktop\\Big Data\\Datasets\\books.csv");

        bookDS.show(5);

        Dataset<Row> ratingsDS = sparkSession.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("C:\\Users\\Master\\Desktop\\Big Data\\Datasets\\ratings.csv");

        ratingsDS.show(5);

        bookDS.join(ratingsDS, "book_id").show(5);*/
    }
}
