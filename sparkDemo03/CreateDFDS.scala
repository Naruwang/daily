package com.datatist.sparkDemo03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
/**
  * 使用手动指定列名的方式将RDD转成DF
  */
object CreateDFDS {
  def main(args: Array[String]): Unit = {
    //创建SparkSession
    val spark = SparkSession.builder().
      appName("SparkSQL").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    //读取文件
    val fileRDD = sc.textFile("D:\\data\\person.txt")
    val lineRDD = fileRDD.map(_.split(" "))
    val rowRDD: RDD[(Int, String, Int)] = lineRDD.map(line => (line(0).toInt,line(1),line(2).toInt))

    //将RDD转成DF
    //注意，RDD中原本没有toDF方法，新版本中要给他增加一个方法，可以使用隐式转换
    import spark.implicits._
    val personDF = rowRDD.toDF("id","name","age")
    personDF.show(10)
    personDF.printSchema()
    sc.stop()
    spark.stop()

  }
}
