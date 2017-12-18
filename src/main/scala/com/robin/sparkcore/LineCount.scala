package com.robin.sparkcore

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with IntelliJ IDEA.
  * Description: 
  * User: bin
  * Date: 2017-12-18
  *
  */
object LineCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LineCount").setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("C:\\idea\\spark_demo\\src\\main\\scala\\com\\robin\\file\\hello.txt")
    val pairs = lines.map( line => (line, 1))
    val lineCounts = pairs.reduceByKey(_+_)
    lineCounts.foreach(lineCount => println(lineCount._1 + " appears " + lineCount._2 + " times."))
  }
}


