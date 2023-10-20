package com.bigdatacompany.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class FirstExam {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\bigdata\\hadoop");

        JavaSparkContext javaSparkContext = new JavaSparkContext("local", "First Exam Spark");

        // Load data
        JavaRDD<String> firstData = javaSparkContext.textFile("C:\\Users\\Polat\\Desktop\\BigData\\Datasets\\first-data.txt");

        System.out.println(firstData.count());
        System.out.println(firstData.first());

        // List to RDD
        List<String> data = Arrays.asList("big data", "elasticsearch", "first", "spark", "hadoop");
        JavaRDD<String> secondData = javaSparkContext.parallelize(data);

        System.out.println("# Total data: " + secondData.count());
        System.out.println("First data: " + secondData.first());

    }
}
