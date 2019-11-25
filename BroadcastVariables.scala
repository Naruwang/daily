package com.datatis.sparkDemo02

import org.apache.spark.{SparkConf, SparkContext}

//广播变量，各个executor共享数据
object BroadcastVariables {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("bv")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    //不适用广播变量
    val kvFruit = sc.parallelize(List((1,"apple"),(2,"orange"),(3,"banana"),(4,"grape")))
    val fruitMap = kvFruit.collectAsMap()
    val fruitIds = sc.parallelize(List(2,4,1,3))
    //根据水果id取水果名称
    val fruitName = fruitIds.map( x => fruitMap(x))
    fruitName.foreach(println)

    /*
    以上代码看似一点问题没有，但是考虑到数据量如果较大，且Task数较多
    那么会导致，被各个Task共用到的fruitMap会被多次传输
    应该要减少fruitMap的传输，一台机器一个，被该台机器中的Task共用即可
    使用广播变量
     */
    println("------------------")
    val broadCastFruitMap = sc.broadcast(fruitMap)
    val broadCastFruitName = fruitIds.map(x => broadCastFruitMap.value(x))
    broadCastFruitName.foreach(println)
  }
}
