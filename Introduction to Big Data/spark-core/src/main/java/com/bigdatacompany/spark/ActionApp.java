package com.bigdatacompany.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

public class ActionApp {
    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir", "C:\\hadoop");

        JavaSparkContext javaSparkContext = new JavaSparkContext("local", "Map Transformation Spark");

        JavaRDD<String> rawData = javaSparkContext.textFile("C:\\Users\\Master\\Desktop\\Big Data\\Datasets\\person.csv");

        System.out.println(rawData.count());
        System.out.println(rawData.first());
        System.out.println(rawData.take(3));

        /*rawData.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });*/
    }
}
