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
object ManuallySpecifyOptions {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","C:\\hadoop-2.7.0")
    val conf = new SparkConf()
      .setAppName("ManuallySpecifyOptions")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val peopleDF = sqlContext.read.format("json").load("C:\\idea\\spark_demo\\src\\main\\scala\\com\\robin\\file\\people.json")
    peopleDF.select("name").write.mode("overwrite").format("parquet").save("C:\\idea\\spark_demo\\src\\main\\scala\\com\\robin\\file\\peopleName_scala")

  }

}
