package com.robin.sparkcore

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with IntelliJ IDEA.
  * Description: 
  * User: bin
  * Date: 2017-12-19
  *
  */
object SecondSort {
  def main(args: Array[String]): Unit = {
    def test0(): Unit ={
      val lines = "a\t1"
      println(lines.split("\t")(0))
    }

    def test1():Unit={
      val conf = new SparkConf()
        .setAppName("SecondSort")
        .setMaster("local")
      val sc = new SparkContext(conf)
      val lines = sc.textFile("C:\\idea\\spark_demo\\src\\main\\scala\\com\\robin\\file\\sort.txt",1)
      val secondSort = lines
        .map(line => (line.split("\t")(0), line.split("\t")(1)).swap)
        .sortByKey(false)
        .map(word => (word._1, word._2).swap)
        .sortByKey(false)
      secondSort.foreach(word => println(word._1 + " appears : " + word._2 + " times."))

    }

    def test2():Unit={
      val conf = new SparkConf()
        .setAppName("SecondSort")
        .setMaster("local")
      val sc = new SparkContext(conf)
      val lines = sc.textFile("C:\\idea\\spark_demo\\src\\main\\scala\\com\\robin\\file\\sort.txt",1)
      val secondSort = lines
        .map(line => (line.split("\t")(0), line.split("\t")(1)).swap)
        .sortByKey(false)
        .map(word => (word._2, word._1))
        .sortByKey(false)
      secondSort.foreach(word => println(word._1 + " appears : "+ word._2 + " times."))
    }

    def test3():Unit={
      val conf = new SparkConf()
        .setAppName("SecondSort")
        .setMaster("local")
      val sc = new SparkContext(conf)
      val lines = sc.textFile("C:\\idea\\spark_demo\\src\\main\\scala\\com\\robin\\file\\sort2.txt",1)
      val pairs = lines.map(line => (
       new SecondSortKey(line.split(" ")(0).toInt, line.split(" ")(1).toInt),line
      ))
      val sortedPairs = pairs.sortByKey()
      val sortedLines = sortedPairs.map(_._2)
      sortedLines.foreach(println(_))
    }


//    test0()
//    test1()
//    test2()
    test3()



  }
}
