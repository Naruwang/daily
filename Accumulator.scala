package com.datatis.sparkDemo02

import org.apache.spark.rdd.RDD
import org.apache.spark.{ SparkConf, SparkContext}

object Accumulator {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("accumulator")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    //使用Scala完成累加
    var count1: Int = 0
    var data = Seq(1,2,3)
    data.foreach(x => count1 += x)
    println("count1:" + count1)

    //使用RDD累加
    var count2 = 0
    val dataRDD: RDD[Int] = sc.parallelize(data)
    dataRDD.foreach(x => count2 += x)
    println("count2:" + count2)
    //上面的RDD操作运行结果是0，因为foreach中的函数式传递给Worker中的Executor执行
    //用到了count2变量，而count2变量在Driver端定义的，在传递给Executor的时候，各个Executor都有了一份count2
    //最后各个Executor将各个x加到了自己的count2上面了，和Driver端的count2没有关系，使用累加器解决

    var count3 = sc.accumulator(0)
    dataRDD.foreach(x => count3 += x)
    println("count3:" + count3)
  }
}
