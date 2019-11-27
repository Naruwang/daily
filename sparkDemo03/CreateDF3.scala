package com.datatist.sparkDemo03

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 使用样例类+反射的方式将RDD转成DF
  */
object CreateDF3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("wc")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    val fileRDD = sc.textFile("D:\\data\\person.txt")
    val lineRDD = fileRDD.map(_.split(" "))
    val rowRDD: RDD[Person] = lineRDD.map(line => Person(line(0).toInt,line(1),line(2).toInt))
    import spark.implicits._
    val personDF: DataFrame = rowRDD.toDF
    personDF.show(10)
    personDF.printSchema()
    sc.stop()
    spark.stop()
  }
  //person样例类
  case class Person(id:Int,name:String,age:Int)
}
