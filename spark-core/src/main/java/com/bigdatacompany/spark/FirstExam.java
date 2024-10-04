package com.bigdatacompany.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class FirstExam {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\bigdata\\hadoop");

        JavaSparkContext sc = new JavaSparkContext("local", "First Exam Spark");

        // Load data
        JavaRDD<String> firstData = sc.textFile("C:\\Users\\Pantheon\\Desktop\\BigData\\Datasets\\first-data.txt");
        System.out.println(firstData.count());
        System.out.println(firstData.first());

        // List to RDD
        List<String> data = Arrays.asList("big data", "elasticsearch", "first", "spark", "hadoop");
        JavaRDD<String> secondData = sc.parallelize(data);
        System.out.println("Number of data: " + secondData.count());
        System.out.println("First data: " + secondData.first());

    }
}
