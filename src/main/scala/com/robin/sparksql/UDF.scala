package com.robin.sparksql


import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with IntelliJ IDEA.
  * Description: 
  * User: bin
  * Date: 2017-12-22
  *
  */
object UDF {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","C:\\hadoop-2.7.0")
    val conf = new SparkConf()
      .setAppName("UDF")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // 构造模拟数据
    val names = Array(
      "Leo",
      "Marry",
      "Jack",
      "Robin"
    )
    val namesRDD = sc.parallelize(names, 2)
    val namesRowRDD = namesRDD.map(name => Row(name))
    val structType = StructType(
      Array(
        StructField("name", StringType, true)
      )
    )
    val namesDF = sqlContext.createDataFrame(namesRowRDD, structType)
    namesDF.registerTempTable("names")

    // 定义和注册自定义函数
    // 定义函数：自己写匿名函数
    // 注册函数：SQLContext.udf.register()
    sqlContext.udf.register("strlen", (str: String) =>{str.length()})

    //使用注册函数
    sqlContext.sql(
      s"""
         |SELECT  name
         |       ,strlen(name)
         |  FROM names
       """.stripMargin)
      .foreach(row => println(row))
  }
}
