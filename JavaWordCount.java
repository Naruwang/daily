package com.datatist.sparkDemo1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
/*
官网例子
JavaRDD<String> textFile = sc.textFile("hdfs://...");
JavaPairRDD<String, Integer> counts = textFile
    .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
    .mapToPair(word -> new Tuple2<>(word, 1))
    .reduceByKey((a, b) -> a + b);
counts.saveAsTextFile("hdfs://...");
 */
public class JavaWordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("wc").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> fileJavaRDD = jsc.textFile("D:\\data\\words.txt");
        JavaRDD<String> wordRDD = fileJavaRDD.flatMap(file -> Arrays.asList(file.split(" ")).iterator());
        JavaPairRDD<String, Integer> wordAndOne = wordRDD.mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairRDD<String, Integer> result = wordAndOne.reduceByKey((a, b) -> (a + b));
        result.collect().forEach(System.out::println);
    }
}
