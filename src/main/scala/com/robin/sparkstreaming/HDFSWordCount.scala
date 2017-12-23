package com.robin.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object HDFSWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("HDFSWordCount")
      .setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(5))
    val lines = ssc.textFileStream("D:\\workspace\\spark_demo\\src\\main\\scala\\com\\robin\\file\\test1.txt")
    val words = lines.flatMap(line => line.split(","))
    val pairs = words.map(word => (word,1))
    val wordCounts = pairs.reduceByKey(_+_)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()

  }

}
