package com.datatist.sparkDemo03


import java.sql.{Connection, DriverManager}

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object JdbcTest {



  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("jdbc")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    //插入数据
    val data = sc.parallelize(List(("jack", 18), ("tom", 19), ("rose", 20)))
    data.foreachPartition(savaToMySQL)

    //获取连接
    def getConn():Connection={
      DriverManager.getConnection("" +
        "jdbc:mysql://localhost:3306/bigdata?characterEncoding=UTF-8","root","root")}

    //读取数据
    /*
    class JdbcRDD[T: ClassTag](
    sc: SparkContext,
    getConnection: () => Connection,
    sql: String,
    lowerBound: Long,
    upperBound: Long,
    numPartitions: Int,
    mapRow: (ResultSet) => T = JdbcRDD.resultSetToObjectArray _)
  extends RDD[T](sc, Nil) with Logging {
     */
    val studentRDD = new JdbcRDD(sc,
      getConn,
      sql = "select * from t_student where id >= ? and id <= ?",
      4,
      6,
      2,
      rs => {
        val id = rs.getInt("id")
        val name = rs.getString("name")
        val age = rs.getInt("age")
        (id, name, age)
      }
    )
    //打印读取的数据
    println(studentRDD.collect().toBuffer)
  }

  //保存数据到数据库
  def savaToMySQL( partitionData:Iterator[(String, Int)] ): Unit = {
    //将数据存入到Mysql
    //获取连接
    val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata?characterEncoding=UTF-8", "root", "root")
    partitionData.foreach(data => {
      val sql = "INSERT INTO `t_student` (`id`, `name`, `age`) VALUES (NULL, ?, ?);"
      val ps = conn.prepareStatement(sql)
      ps.setString(1,data._1)
      ps.setInt(2,data._2)
      ps.execute()
    })
    //关闭连接
    conn.close()
  }
}
