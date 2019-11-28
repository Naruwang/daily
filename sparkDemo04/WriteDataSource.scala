package com.datatist.sparkDemo04



import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * 将DF/DS中的数据写入到不同的数据源
  */
object WriteDataSource {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("writedata")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    val fileRDD: RDD[String] = sc.textFile("D:\\data\\person.txt")
    val lineRDD: RDD[Array[String]] = fileRDD.map(_.split(" "))
    val rowRDD: RDD[Person] = lineRDD.map(line => Person(line(0).toInt,line(1),line(2).toInt))
    import spark.implicits._
    val personDF: DataFrame = rowRDD.toDF()

    //讲DF写入到不同数据源
    //personDF.write.text("D:\\data\\output\\text")
    //Text data source supports only a single column, and you have 3 columns.;
    personDF.write.json("D:\\data\\output\\json")
    personDF.write.csv("D:\\data\\output\\csv")
    personDF.write.parquet("D:\\data\\output\\parquet")

    val prop = new Properties()
    prop.setProperty("user","root")
    prop.setProperty("password","root")
    personDF.write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://localhost:3306/bigdata?characterEncoding=UTF-8","person",prop)
    println("写入成功")
    sc.stop()
    spark.stop()
  }
  case class Person(id:Int,name:String,age:Int)
}
