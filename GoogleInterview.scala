package com.datatis.sparkDemo02
//有N阶楼梯 ,你每次只能爬1或2阶楼梯；能有多少种爬法
object GoogleInterview {
  def main(args: Array[String]): Unit = {
    println(getCount(5))
  }
  //定义一个方法传入楼梯级数，返回爬楼梯种数
  def getCount(n:Int):Int = {
    if(n == 1){
      1
    }else if(n == 2){
      2
    }else{
      getCount(n - 1) + getCount(n - 2)
    }
  }
}
