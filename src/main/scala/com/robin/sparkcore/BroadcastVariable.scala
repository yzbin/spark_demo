package com.robin.sparkcore

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with IntelliJ IDEA.
  * Description: 
  * User: bin
  * Date: 2017-12-18
  *
  */
object BroadcastVariable {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("BroadcastVariable")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val factor = 3
    val factorBroadcast = sc.broadcast(3)

    val numberArray = Array(1, 2, 3, 4, 5)
    val numbers = sc.parallelize(numberArray, 1)
    val multipleNumbers = numbers.map( num => num * factorBroadcast.value )
    multipleNumbers.foreach(num => println(num))

  }

}
