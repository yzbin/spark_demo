package com.robin.sparksql

import breeze.linalg.sum
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext, sql}

/**
  * Created with IntelliJ IDEA.
  * Description: 
  * User: bin
  * Date: 2017-12-20
  *
  */
object DailySale {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("DailySale")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    // 模拟数据
    val userSaleLog = Array(
      "2015-10-01,55.05,1122",
      "2015-10-01,23.15,1133",
      "2015-10-01,15.20,",
      "2015-10-02,56.05,1144",
      "2015-10-02,78.87,1155",
      "2015-10-02,113.02,1123")
    val userSaleRDD = sc.parallelize(userSaleLog, 5)
    val filteredUserSaleRDD = userSaleRDD.
      filter{log => if(log.split(",").length == 3) true else  false}

    val userSaleRowRDD = filteredUserSaleRDD.
      map{ log => Row(log.split(",")(0),log.split(",")(1).toDouble)}

    val structType = StructType(
      Array(
        StructField("date", StringType, false),
        StructField("sale_male", DoubleType, false)
      )
    )

    val userSaleDF = sqlContext.createDataFrame(userSaleRowRDD, structType)
    userSaleDF.registerTempTable("temp")
    userSaleDF.show()
    sqlContext.sql(
      s"""
         |SELECT date
         |      ,SUM(sale_male)
         |  FROM temp
         | GROUP BY date
       """.stripMargin).show()

  }

}
