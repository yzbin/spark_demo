package com.robin.sparkcore

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with IntelliJ IDEA.
  * Description: 
  * User: bin
  * Date: 2017-12-18
  *
  */
object SortWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SortWordCount").setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("C:\\idea\\spark_demo\\src\\main\\scala\\com\\robin\\file\\hello.txt")
    val words = lines.flatMap(line => line.split(" "))
    val pairs = words.map(word => (word,1))
    val wordCounts = pairs.reduceByKey(_+_)
    val countWords = wordCounts.map( wordCount => (wordCount._2, wordCount._1))
    val sortedCountWords = countWords.sortByKey(false)
    val sortedWordCounts = sortedCountWords.map( sortedCountWord => (sortedCountWord._2, sortedCountWord._1))
    sortedWordCounts.foreach( sortWordCount =>{
      println(sortWordCount._1 + " appears : " + sortWordCount._2 + " times.")
    })

  }
}
