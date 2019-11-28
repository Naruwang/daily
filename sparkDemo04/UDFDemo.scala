package com.datatist.sparkDemo04

import org.apache.spark.sql.SparkSession

object UDFDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("udf")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val fileDS = spark.read.textFile("D:\\data\\udf.txt")
    fileDS.show()

    fileDS.createOrReplaceTempView("t_word")
    /**
      * +----------+
      * |     value|
      * +----------+
      * |helloworld|
      * |       abc|
      * |     study|
      * | smallWORD|
      * +----------+
      */
    /**
      * 将每一行数据转换成大写
      * select value,smallToBig(value) from t_word
      */
    val func = (str:String) => {str.toUpperCase()}
    spark.udf.register("smallToBig",func)
    spark.sql("select value,smallToBig(value) from t_word").show()

    /**
      * +----------+---------------------+
      * |     value|UDF:smallToBig(value)|
      * +----------+---------------------+
      * |helloworld|           HELLOWORLD|
      * |       abc|                  ABC|
      * |     study|                STUDY|
      * | smallWORD|            SMALLWORD|
      * +----------+---------------------+
      */
    sc.stop()
    spark.stop()
  }
}
