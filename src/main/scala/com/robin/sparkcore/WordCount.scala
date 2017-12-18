package com.robin.sparkcore

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with IntelliJ IDEA.
  * Description: 
  * User: bin
  * Date: 2017-12-18
  *
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WrodCount").setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("C:\\idea\\spark_demo\\src\\main\\scala\\com\\robin\\file\\hello.txt", 1)
    val words = lines.flatMap(line => line.split(" "))
    val pairs = words.map(word => (word,1))
    val wordCounts = pairs.reduceByKey(_+_)
    wordCounts.foreach( wordcount => println(wordcount._1 + " appears : " + wordcount._2 + " times"))
  }

}
