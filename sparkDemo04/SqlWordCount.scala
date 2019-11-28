package com.datatist.sparkDemo04

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SqlWordCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("wc").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    val fileDF: DataFrame = spark.read.text("D:\\data\\words.txt")
    val fileDS: Dataset[String] = spark.read.textFile("D:\\data\\words.txt")
    import spark.implicits._
    val wordDS = fileDS.flatMap(_.split(" "))
    //wordDS.show()
    /**
      * +-----+
      * |value|
      * +-----+
      * |hello|
      * |   me|
      * |hello|
      * |  you|
      * ...
      */
    //对上面的数据进行wordcount,先注册成表，注意默认字段是value
    wordDS.createOrReplaceTempView("person")

    val sql =
      """
        |select value,count(value) as count
        |from person
        |group by value
        |order by count desc
      """.stripMargin
    spark.sql(sql).show()

    /**
      * +-----+-----+
      * |value|count|
      * +-----+-----+
      * |hello|   12|
      * |  her|    4|
      * |   me|    4|
      * |  you|    4|
      * +-----+-----+
      */
    sc.stop()
    spark.stop()
  }
}
