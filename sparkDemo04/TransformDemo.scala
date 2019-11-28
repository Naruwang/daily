package com.datatist.sparkDemo04

import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * RDD、DataFrame、DataSet间的相互转换
  */
object TransformDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("transform").master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    val fileRDD = sc.textFile("D:\\data\\person.txt")
    val lineRDD = fileRDD.map(_.split(" "))
    val rowRDD = lineRDD.map(line => Person(line(0).toInt,line(1),line(2).toInt))
    import spark.implicits._
    //RDD --> DF
    val personDF = rowRDD.toDF()

    //DF --> RDD
    val personRDD = personDF.rdd

    //RDD --> DS
    val personDS = rowRDD.toDS()

    //DS --> RDD
    val personRDD2 = personDS.rdd

    //DF --> DS
    val personDS2: Dataset[Person] = personDF.as[Person]

    //DS --> DF
    val DF = personDS2.toDF()
    sc.stop()
    spark.stop()
  }
  case class Person(id:Int,name:String,age:Int)
}
