package com.datatist.sparkDemo03

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

/**
  * 使用手动指定StructType的方式将RDD转成DF
  */
object CreateDF2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    val fileRDD: RDD[String] = sc.textFile("D:\\data\\person.txt")
    val lineRDD = fileRDD.map(_.split(" "))
    val rowRDD = lineRDD.map(line => Row(line(0).toInt,line(1),line(2).toInt))

    /**
      * * val innerStruct =
      * *   StructType(
      * *     StructField("f1", IntegerType, true) ::
      * *     StructField("f2", LongType, false) ::
      * *    StructField("f3", BooleanType, false) :: Nil)
      */
    val schema = StructType(Seq(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true)
    ))
    val personDF = spark.createDataFrame(rowRDD,schema)
    personDF.show(10)
    personDF.printSchema()
    sc.stop()
    spark.stop()
  }
}
