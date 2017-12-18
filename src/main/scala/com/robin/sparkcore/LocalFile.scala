package com.robin.sparkcore

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with IntelliJ IDEA.
  * Description: 
  * User: bin
  * Date: 2017-12-18
  *
  */
object LocalFile {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LocalFile").setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("C:\\idea\\spark_demo\\src\\main\\scala\\com\\robin\\file\\hello.txt")
    val count = lines.map(line => line.length()).reduce(_+_)
    println("file's count is " + count)
  }
}
