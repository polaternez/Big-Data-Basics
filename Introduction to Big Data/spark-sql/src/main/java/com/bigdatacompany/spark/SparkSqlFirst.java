package com.bigdatacompany.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class SparkSqlFirst {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\hadoop");

        StructType schema = new StructType()
                .add("first_name", DataTypes.StringType)
                .add("last_name", DataTypes.StringType)
                .add("email", DataTypes.StringType)
                .add("gender", DataTypes.StringType)
                .add("country", DataTypes.StringType)
                .add("age", DataTypes.IntegerType);

        SparkSession sparkSession = SparkSession.builder().master("local").appName("First Exam").getOrCreate();

        Dataset<Row> rawDS = sparkSession.read()
                .option("header", true)
                .schema(schema)
                .csv("C:\\Users\\Master\\Desktop\\Big Data\\Datasets\\person.csv");

        rawDS.show();
        rawDS.printSchema();

        // --select--

       /* Dataset<Row> selectDS = rawDS.select("first_name", "last_name");
        selectDS.show((int)selectDS.count());*/

        // --filter--

        Dataset<Row> selDS = rawDS.select("first_name", "last_name", "email", "country", "age");
/*
//        Dataset<Row> chinaDS = selDS.filter(selDS.col("country").equalTo("China"));
        Dataset<Row> chinaDS = selDS.filter("country = 'China'");

//        Dataset<Row> oldChineseDS = selDS.filter(selDS.col("country").equalTo("China")
//                .and(selDS.col("age").gt(50))
//                .and(selDS.col("email").contains("google")));
        Dataset<Row> oldChineseDS = selDS.filter("country ='China' AND age > 50 AND email like '%google%'");

//        Dataset<Row> countryDS = selDS.filter(selDS.col("country").equalTo("France").or(selDS.col("country").equalTo("Brazil")));
        Dataset<Row> countryDS = selDS.filter("country='France' or country='Brazil'");

        countryDS.show();*/

        // --sort--

/*//        Dataset<Row> ageGt50DS = selDS.filter("age >= 50").sort("age");
        Dataset<Row> ageGt50DS = selDS.filter("age >= 50").sort(functions.desc("age"));

        ageGt50DS.show();*/

        // --withColumn--
/*
//        Dataset<Row> wcDS = rawDS.withColumn("first_name_test", rawDS.col("first_name"));
        Dataset<Row> wcDS = rawDS.withColumn("gender_new",
                when(functions.col("gender").equalTo("Female"), 0)
                .when(functions.col("gender").equalTo("Male"), 1)
                        .otherwise(2));

        wcDS.show();*/

        // --groupBy--

       /* Dataset<Row> countryGroupDS = selDS.groupBy("country").count();
        countryGroupDS.show();*/

    }
}
