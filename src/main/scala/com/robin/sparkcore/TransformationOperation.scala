package com.robin.sparkcore

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with IntelliJ IDEA.
  * Description: 
  * User: bin
  * Date: 2017-12-18
  *
  */
object TransformationOperation {
  def main(args: Array[String]): Unit = {
//    map()
//    filter()
//    flatMap()
//    groupByKey()
//    reduceByKey()
//    sortByKey()
    join()
  }

  def map(): Unit ={
    val conf = new SparkConf().setAppName("map").setMaster("local")
    val sc = new SparkContext(conf)
    val numberArray = Array(1, 2, 3, 4, 5)
    val numbers = sc.parallelize(numberArray, 1)
    val multipleNumbers = numbers.map(num => num * 2)
    multipleNumbers.foreach(num => println(num))

  }

  def filter(): Unit ={
    val conf = new SparkConf().setAppName("filter").setMaster("local")
    val sc = new SparkContext(conf)
    val numberArray = Array(1, 2, 3, 4, 5, 6, 7, 8, 9 ,10)
    val numbers = sc.parallelize(numberArray, 1)
    val evenNumbers = numbers.filter( num => num % 2 == 0)
    evenNumbers.foreach(num => println(num))
  }

  def flatMap(): Unit ={
    val conf = new SparkConf().setAppName("flatMap").setMaster("local")
    val sc = new SparkContext(conf)
    val linesArray = Array("hello you", "hello me", "hello world")
    val lines = sc.parallelize(linesArray,1)
    val words = lines.flatMap( line => line.split(" "))
    words.foreach( num => println(num))
  }

  def groupByKey(): Unit ={
    val conf = new SparkConf().setAppName("groupByKey").setMaster("local")
    val sc = new SparkContext(conf)

    val scoreList = Array(Tuple2("class1", 90), Tuple2("class2", 80), Tuple2("class1", 75), Tuple2("class2", 60))
    val scores = sc.parallelize(scoreList,1)
    val groupedScores = scores.groupByKey()

    groupedScores.foreach( score => {
      println(score._1)
      score._2.foreach{
        singleScore => println(singleScore)
      }
      println("==============================")
    })
  }

  def reduceByKey(): Unit ={
    val conf = new SparkConf().setAppName("reduceByKey").setMaster("local")
    val sc = new SparkContext(conf)

    val scoreList = Array(Tuple2("class1", 90), Tuple2("class2", 80), Tuple2("class1", 75), Tuple2("class2",60))
    val scores = sc.parallelize(scoreList,1)
    val totalScores = scores.reduceByKey(_+_)
    totalScores.foreach(classScore => println(classScore._1 + ":" + classScore._2))

  }

  def sortByKey(): Unit ={
    val conf = new SparkConf().setAppName("sortByKey").setMaster("local")
    val sc = new SparkContext(conf)
    val scoreList = Array(Tuple2(65, "leo"), Tuple2(50, "tom"), Tuple2(100, "marry"), Tuple2(85, "tom"))
    val scores = sc.parallelize(scoreList, 1)
    val sortedScores = scores.sortByKey(false)
    sortedScores.foreach(studentScore => println(studentScore._1 + ": " + studentScore._2))
  }

  def join(): Unit = {
    val conf = new SparkConf().setAppName("join").setMaster("local")
    val sc = new SparkContext(conf)

    val studetList = Array(
      Tuple2(1, "leo"),
      Tuple2(2, "jack"),
      Tuple2(3, "tom")
    )

    val scoreList = Array(
      Tuple2(1, 100),
      Tuple2(2, 90),
      Tuple2(3, 60)
    )

    val students = sc.parallelize(studetList, 1)
    val scores = sc.parallelize(scoreList, 1)

    val studentScores = students.join(scores)
    studentScores.foreach(studentScore => {
      println("student id :" + studentScore._1)
      println("student name :" + studentScore._2._1)
      println("student score :" + studentScore._2._2)
      println("========================================")
    })
  }


  def cogroup() {

  }
}
