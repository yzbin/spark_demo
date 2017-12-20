package com.robin.sparksql


import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with IntelliJ IDEA.
  * Description: 
  * User: bin
  * Date: 2017-12-20
  *
  */
object DailyUV {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("DailyUV")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    val userAccessLog = Array(
      "2015-10-01,1122",
      "2015-10-01,1122",
      "2015-10-01,1123",
      "2015-10-01,1124",
      "2015-10-01,1124",
      "2015-10-02,1122",
      "2015-10-02,1121",
      "2015-10-02,1123",
      "2015-10-02,1123")
    val userAccessLogRDD = sc.parallelize(userAccessLog, 5)
    val userAccessLogRowRDD = userAccessLogRDD
      .map{log => Row(log.split(",")(0), log.split(",")(1).toInt)}

    val structType = StructType(
      Array(
        StructField("date", StringType, true),
        StructField("userid", IntegerType, true)
      )
    )
    val userLogAccessRowDF = sqlContext.createDataFrame(userAccessLogRowRDD, structType)
    userLogAccessRowDF.show()
    userLogAccessRowDF.printSchema()
    userLogAccessRowDF.registerTempTable("temp")
    sqlContext.sql(
      s"""
         |SELECT  date
         |       ,COUNT(userid) AS usr_cnt
         |  FROM temp
         |GROUP BY date
       """.stripMargin).show()

  }
}
