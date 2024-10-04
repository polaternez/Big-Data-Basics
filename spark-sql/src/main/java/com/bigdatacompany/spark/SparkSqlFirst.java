package com.bigdatacompany.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

public class SparkSqlFirst {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\bigdata\\hadoop");

        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("First Exam")
                .getOrCreate();

        StructType schema = new StructType()
                .add("first_name", DataTypes.StringType)
                .add("last_name", DataTypes.StringType)
                .add("email", DataTypes.StringType)
                .add("gender", DataTypes.StringType)
                .add("country", DataTypes.StringType)
                .add("age", DataTypes.IntegerType);
        Dataset<Row> personDF = spark.read()
                .option("header", true)
                .schema(schema)
                .csv("C:\\Users\\Pantheon\\Desktop\\BigData\\Datasets\\person.csv");
        personDF.show();
        personDF.printSchema();

        // --select--
        /*Dataset<Row> selectDF = personDF.select("first_name", "last_name");
        selectDF.show((int)selectDF.count()); // display all rows */

        // --filter--
        Dataset<Row> selectDF = personDF.select("first_name", "last_name", "email", "country", "age");

/*//        Dataset<Row> chinaDF = selectDF.filter("country = 'China'");
        Dataset<Row> chinaDF = selectDF.filter(
                selectDF.col("country").equalTo("China")
        );
        chinaDF.show();*/

/*//        Dataset<Row> oldChineseDF = selectDF.filter("country='China' AND age>50 AND email like '%google%'");
        Dataset<Row> oldChineseDF = selectDF.filter(
                selectDF.col("country").equalTo("China")
                        .and(selectDF.col("age").gt(50))
                        .and(selectDF.col("email").contains("google"))
        );
        oldChineseDF.show();*/

/*//        Dataset<Row> countryDF = selectDF.filter("country='France' or country='Brazil'");
        Dataset<Row> countryDF = selectDF.filter(
                selectDF.col("country").equalTo("France")
                        .or(selectDF.col("country").equalTo("Brazil"))
        );
        countryDF.show();*/

        // --sort--
        Dataset<Row> ageGt50DF = selectDF.filter("age >= 50")
                .sort("age");
        ageGt50DF.show();

        // --Add new column--
        /*Dataset<Row> wcDF = personDF
                .withColumn("first_name_test", lower(col("first_name")))
                .withColumn("age_test", expr("age * age"));*/
        /*Dataset<Row> wcDF = personDF.withColumn("gender_new",
                when(functions.col("gender").equalTo("Female"), 0)
                        .when(functions.col("gender").equalTo("Male"), 1)
                        .otherwise(2)
        );
        wcDF.show();*/


        // --groupBy--
        Dataset<Row> countDF = selectDF.groupBy("country").count()
                .sort(functions.desc("count"));
        countDF.show();

    }
}
