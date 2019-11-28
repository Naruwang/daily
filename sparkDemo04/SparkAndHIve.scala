package com.datatist.sparkDemo04

import org.apache.spark.sql.SparkSession

object SparkAndHIve {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("sparkandhive")
      .master("local[*]")
      //.config("spark.sql.warehouse.dir", "hdfs://node01:9000/user/hive/warehouse")
      //.config("hive.metastore.uris", "thrift://node01:9083")
      .enableHiveSupport()
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("OFF")

    spark.sql("show tables").show()
    spark.stop()
  }

}
