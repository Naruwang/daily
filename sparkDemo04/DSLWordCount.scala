package com.datatist.sparkDemo04

import org.apache.spark.sql.SparkSession

/**
  * DSL风格完成WordCount
  */
object DSLWordCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("dslwc")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    val fileDS = spark.read.textFile("D:\\data\\words.txt")
    import spark.implicits._
    val wordDS = fileDS.flatMap(_.split(" "))
    wordDS.show()

    /**
      * +-----+
      * |value|
      * +-----+
      * |hello|
      * |   me|
      * |hello|
      * ...
      */
    wordDS.groupBy("value").count().orderBy($"count".desc).show()
    sc.stop()
    spark.stop()
  }
}
