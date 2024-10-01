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
        Dataset<Row> rawDS = spark.read()
                .option("header", true)
                .schema(schema)
                .csv("C:\\Users\\Pantheon\\Desktop\\BigData\\Datasets\\person.csv");
        rawDS.show();
        rawDS.printSchema();

        // --select--
        /*Dataset<Row> selectDS = rawDS.select("first_name", "last_name");
        selectDS.show((int)selectDS.count()); // display all rows */

        // --filter--
        Dataset<Row> selDS = rawDS.select("first_name", "last_name", "email", "country", "age");

/*//        Dataset<Row> chinaDS = selDS.filter("country = 'China'");
        Dataset<Row> chinaDS = selDS.filter(
                selDS.col("country").equalTo("China")
        );
        chinaDS.show();*/

/*//        Dataset<Row> oldChineseDS = selDS.filter("country ='China' AND age > 50 AND email like '%google%'");
        Dataset<Row> oldChineseDS = selDS.filter(
                selDS.col("country").equalTo("China")
                        .and(selDS.col("age").gt(50))
                        .and(selDS.col("email").contains("google"))
        );
        oldChineseDS.show();*/

/*//        Dataset<Row> countryDS = selDS.filter("country='France' or country='Brazil'");
        Dataset<Row> countryDS = selDS.filter(
                selDS.col("country").equalTo("France")
                        .or(selDS.col("country").equalTo("Brazil"))
        );
        countryDS.show();*/

        // --sort--
        /*Dataset<Row> ageGt50DS = selDS.filter("age >= 50")
                .sort("age");*/
        Dataset<Row> ageGt50DS = selDS.filter("age >= 50")
                .sort(functions.desc("age"));
        ageGt50DS.show();

        // --Add new column--
        /*Dataset<Row> wcDS = rawDS
                .withColumn("first_name_test", lower(col("first_name")))
                .withColumn("age_test", expr("age * age"));*/
        /*Dataset<Row> wcDS = rawDS.withColumn("gender_new",
                when(functions.col("gender").equalTo("Female"), 0)
                        .when(functions.col("gender").equalTo("Male"), 1)
                        .otherwise(2)
        );
        wcDS.show();*/


        // --groupBy--
        Dataset<Row> countryGroupDS = selDS.groupBy("country").count()
                .sort(functions.desc("count"));
        countryGroupDS.show();

    }
}
