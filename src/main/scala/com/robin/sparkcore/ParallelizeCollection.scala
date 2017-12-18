package com.robin.sparkcore

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with IntelliJ IDEA.
  * Description: 
  * User: bin
  * Date: 2017-12-18
  *
  */
object ParallelizeCollection {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ParallelizeCollection").setMaster("local")
    val sc = new SparkContext(conf)

    val numberArray = Array(1, 2, 3, 4, 5, 6, 7, 8, 9 ,10)
    val numbers = sc.parallelize(numberArray, 5)
    val sum = numbers.reduce(_+_)
    println("1 到 10 的累加和 : " + sum)
  }

}
