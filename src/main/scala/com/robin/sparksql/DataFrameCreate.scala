package com.robin.sparksql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with IntelliJ IDEA.
  * Description: 
  * User: bin
  * Date: 2017-12-20
  *
  */
object DataFrameCreate {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("DataFrameCreate")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val df = sqlContext.read.json("C:\\idea\\spark_demo\\src\\main\\scala\\com\\robin\\file\\people.json")
    df.show()
  }
}
