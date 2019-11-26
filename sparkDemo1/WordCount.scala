package com.datatist.sparkDemo1

import java.util

import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  /*def main(args: Array[String]): Unit = {
    //1.创建sparkContext
    val conf: SparkConf = new SparkConf().setAppName("wc").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    //2.读取文件
    val fileRDD: RDD[String] = sc.textFile("D:\\data\\words.txt")

    //3.处理数据
    val wordRDD: RDD[String] = fileRDD.flatMap(_.split(" "))
    val wordAndOneRDD: RDD[(String, Int)] = wordRDD.map((_,1))
    val wordCount: RDD[(String, Int)] = wordAndOneRDD.reduceByKey(_+_)

    //4.收集数据
    val result: Array[(String, Int)] = wordCount.collect()
    result.foreach(println)
  }*/

  //集群运行
/*  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wc")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val fileRDD = sc.textFile(args(0))
    val wordRDD = fileRDD.flatMap(_.split(" "))
    val wordAndOneRDD = wordRDD.map((_,1))
    val wordCount = wordAndOneRDD.reduceByKey(_+_)
    wordCount.saveAsTextFile(args(1))
  }*/

  /*
  打成jar包，上传到linux
  然后执行命令提交到Spark-HA集群
  /export/servers/spark/bin/spark-submit \
  --class com.datatist.sparkDemo1.WordCount \
  --master spark://xxx \
  --executor-memeory 1g \
  --total=executor=cores 2 \
  /xxx.jar
  hdfs://xxx
  hdfs://xxx
   */

}
