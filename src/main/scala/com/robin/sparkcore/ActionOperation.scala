package com.robin.sparkcore

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with IntelliJ IDEA.
  * Description: 
  * User: bin
  * Date: 2017-12-18
  *
  */
object ActionOperation {
  def main(args: Array[String]): Unit = {
//    reduce()
//    collect()
//    count()
//    take()
//    countByKey()

  }

  def reduce(): Unit ={
    val conf = new SparkConf().setAppName("reduce").setMaster("local")
    val sc = new SparkContext(conf)
    val numberArray = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val numbers = sc.parallelize(numberArray, 1)
    val sum = numbers.reduce(_+_)
    println(sum)
  }

  def collect(): Unit ={
    val conf = new SparkConf().setAppName("collect").setMaster("local")
    val sc = new SparkContext(conf)

    val numberArray = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val numbers = sc.parallelize(numberArray)
    val doubleNumbers = numbers.map(num => num * 2)
    val doubleNumbersArray = doubleNumbers.collect()
    for (num <- doubleNumbersArray){
      println(num)
    }

  }

  def count(): Unit ={
    val conf = new SparkConf().setAppName("count").setMaster("local")
    val sc = new SparkContext(conf)
    val numberArray = Array(1, 2, 3, 4, 5, 6, 7, 8, 9 ,19)
    val numbers = sc.parallelize(numberArray)
    val count = numbers.count()
    println(count)
  }

  def take(): Unit ={
    val conf = new SparkConf().setAppName("take").setMaster("local")
    val sc = new SparkContext(conf)

    val numberArray = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val numbers = sc.parallelize(numberArray,1)
    val top3Numbers = numbers.take(3)

    for (num <- top3Numbers){
      println(num)
    }

  }

  def saveAsTextFile(): Unit ={

  }

  def countByKey(): Unit ={
    val conf = new SparkConf().setAppName("countByKey").setMaster("local")
    val sc = new SparkContext(conf)

    val studentList = Array(Tuple2("class1","jon"), Tuple2("class2","jack"), Tuple2("class2","robin")
                        ,Tuple2("class2","rose"), Tuple2("class1", "mark"))

    val students = sc.parallelize(studentList, 1)

    val studensCounts = students.countByKey()

    println(studensCounts)


  }





}
