package com.robin.sparksql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with IntelliJ IDEA.
  * Description: 
  * User: bin
  * Date: 2017-12-21
  *
  */
object ParquetLoadData {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","C:\\hadoop-2.7.0")
    val conf = new SparkConf()
      .setAppName("ParquetLoadData")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val usersDF = sqlContext.read.parquet("C:\\idea\\spark_demo\\src\\main\\scala\\com\\robin\\file\\users.parquet")
    usersDF.registerTempTable("users")
    val usersNameDF = sqlContext.sql(
      s"""
         |SELECT  name
         |  FROM users
       """.stripMargin)

    usersNameDF.rdd.map( row => "Name : " + row(0))
      .foreach{username => println(username)}

  }
}
