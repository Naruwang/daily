package com.datatist.sparkDemo03

import org.apache.spark.sql.SparkSession

object QueryTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("spark sql")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    val fileRDD = sc.textFile("D:\\data\\person.txt")
    val lineRDD = fileRDD.map(_.split(" "))
    val rowRDD = lineRDD.map(line => Person(line(0).toInt,line(1),line(2).toInt))
    import spark.implicits._
    val personDF = rowRDD.toDF
    personDF.show(10)
    personDF.printSchema()

    //sql查询
    //注册表
    personDF.createOrReplaceTempView("person")
    //1.查询所有数据
    spark.sql("select * from person").show()
    //2.查询age+1
    spark.sql("select age,age+1 from person").show()
    //3.查询age最大的两个人
    spark.sql("select * from person order by age desc limit 2").show()
    //4.查询各个年龄的人数
    spark.sql("select age,count(*) from person group by age").show()
    //5.查询年龄大于30的
    spark.sql("select * from person where age > 30").show()

    //DSL查询
    //1.查询所有数据
    personDF.select("name","age")
    //2.查询age+1
    personDF.select($"name",$"age"+1)
    //3.查询age最大的两个人
    personDF.sort($"age".desc).show(2)
    //4.查询各个年龄的人数
    personDF.groupBy("age").count().show()
    //5.查询年龄大于30的
    personDF.filter($"age">30).show()

    sc.stop()
    spark.stop()
  }
  case class Person(id:Int,name:String,age:Int)
}
