package com.bigdatacompany.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class SparkProductExam {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\bigdata\\hadoop");

        StructType schema = new StructType()
                .add("first_name", DataTypes.StringType)
                .add("last_name", DataTypes.StringType)
                .add("email", DataTypes.StringType)
                .add("country", DataTypes.StringType)
                .add("price", DataTypes.DoubleType)
                .add("product", DataTypes.StringType);

        SparkSession sparkSession = SparkSession.builder().master("local").appName("First Exam").getOrCreate();
        Dataset<Row> rawDS = sparkSession.read()
                .option("multiline", true)
                .schema(schema)
                .json("C:\\Users\\Polat\\Desktop\\BigData\\Datasets\\product.json");

/*
//        Dataset<Row> countPriceDS = rawDS.groupBy("country", "product").sum("price");
        Dataset<Row> countPriceDS = rawDS.groupBy("country", "product").count();
        countPriceDS.sort(functions.desc("count")).show();*/

  /*      Dataset<Row> countPriceDS = rawDS.groupBy("country").avg("price").sort(functions.desc("avg(price)"));

        countPriceDS.show();*/

        //--SQL API--

        rawDS.createOrReplaceTempView("product");
//        rawDS.createOrReplaceGlobalTempView("product");

        Dataset<Row> sqlDS = sparkSession.sql("select first_name, email, country from product where country='France' or country='China'");

        sqlDS.show();



    }

}
