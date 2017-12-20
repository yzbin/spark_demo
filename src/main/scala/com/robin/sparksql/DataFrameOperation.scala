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
object DataFrameOperation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("DataFrameOperation")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._
    val df = sqlContext.read.json("C:\\idea\\spark_demo\\src\\main\\scala\\com\\robin\\file\\people.json")
    df.show()
    df.printSchema()
    df.select(df("name"))show()
    df.select(df("name"),df("age")+1).show()
    df.filter(df("age")>18).show()
    df.groupBy(df("age")).count().show()
    sc.stop()
  }

}
