package com.datatist.sparkDemo04

import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
  * 读取各种数据源
  */
object ReadDataSource {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("readdata")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    spark.read.json("D:\\data\\output\\json").show()
    spark.read.csv("D:\\data\\output\\csv").toDF("id","name","age").show()  //csv没有id,name,age这一列
    spark.read.parquet("D:\\data\\output\\parquet").show()
    val prop = new Properties()
    prop.setProperty("user","root")
    prop.setProperty("password","root")
    spark.read.jdbc("jdbc:mysql://localhost:3306/bigdata?characterEncoding=UTF-8","person",prop).show()
    sc.stop()
    spark.stop()

    /**
      * +---+---+--------+
      * |age| id|    name|
      * +---+---+--------+
      * | 20|  1|zhangsan|
      * | 29|  2|    lisi|
      * | 25|  3|  wangwu|
      * | 30|  4| zhaoliu|
      * | 35|  5|  tianqi|
      * | 40|  6|    kobe|
      * +---+---+--------+
      *
      * +---+--------+---+
      * | id|    name|age|
      * +---+--------+---+
      * |  1|zhangsan| 20|
      * |  2|    lisi| 29|
      * |  3|  wangwu| 25|
      * |  4| zhaoliu| 30|
      * |  5|  tianqi| 35|
      * |  6|    kobe| 40|
      * +---+--------+---+
      *
      * +---+--------+---+
      * | id|    name|age|
      * +---+--------+---+
      * |  1|zhangsan| 20|
      * |  2|    lisi| 29|
      * |  3|  wangwu| 25|
      * |  4| zhaoliu| 30|
      * |  5|  tianqi| 35|
      * |  6|    kobe| 40|
      * +---+--------+---+
      *
      * +---+--------+---+
      * | id|    name|age|
      * +---+--------+---+
      * |  1|zhangsan| 20|
      * |  2|    lisi| 29|
      * |  3|  wangwu| 25|
      * |  4| zhaoliu| 30|
      * |  5|  tianqi| 35|
      * |  6|    kobe| 40|
      * +---+--------+---+
      */
  }

}
