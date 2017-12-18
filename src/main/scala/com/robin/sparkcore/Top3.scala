package com.robin.sparkcore

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with IntelliJ IDEA.
  * Description: 
  * User: bin
  * Date: 2017-12-18
  *
  */
object Top3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Top3").setMaster("local")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("C:\\idea\\spark_demo\\src\\main\\scala\\com\\robin\\file\\top.txt")
    val pairs = lines.map( line => (line.toInt, line))
    val sortedPairs = pairs.sortByKey(false)
    val sortedNumbers = sortedPairs.map(sortedPair => sortedPair._1)
    val top3Number = sortedNumbers.take(3)

    for (num <- top3Number){
      println(num)
    }

  }

}
