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
    def test1():Unit={
      val conf = new SparkConf().setAppName("WrodCount").setMaster("local")
      val sc = new SparkContext(conf)
      val lines = sc.textFile("C:\\idea\\spark_demo\\src\\main\\scala\\com\\robin\\file\\hello.txt", 1)
      val words = lines.flatMap(line => line.split(" "))
      val pairs = words.map(word => (word,1))
      val wordCounts = pairs.reduceByKey(_+_)
      wordCounts.foreach( wordcount => println(wordcount._1 + " appears : " + wordcount._2 + " times"))
    }

    def test2():Unit ={
      val conf = new SparkConf().setAppName("WordCount").setMaster("local")
      val sc = new SparkContext(conf)
      val lines = sc.textFile("C:\\idea\\spark_demo\\src\\main\\scala\\com\\robin\\file\\hello.txt", 1)
      val wcrdd = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
      wcrdd.foreach( word => println(word._1 + " appears : " + word._2 + " times ."))
    }
//    test1()
    test2()
  }

}
