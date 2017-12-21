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
object JSONDataSource {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","C:\\hadoop-2.7.0")
    val conf = new SparkConf()
      .setAppName("JSONDataSource")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val studentScoresDF = sqlContext.read.json("C:\\idea\\spark_demo\\src\\main\\scala\\com\\robin\\file\\students.json")

    studentScoresDF.registerTempTable("student_scores")
    val studentInfoJSONs = Array(
      "{\"name\":\"Leo\", \"age\":18}",
      "{\"name\":\"Marry\", \"age\":17}",
      "{\"name\":\"Jack\", \"age\":19}")
    val studentInfoJSONsRDD = sc.parallelize(studentInfoJSONs, 3)
    val studenInfosDF = sqlContext.read.json(studentInfoJSONsRDD)
    studenInfosDF.registerTempTable("student_infos")
    val goodStudentDF=sqlContext.sql(
      s"""
         |SELECT  s1.name
         |       ,s1.score
         |       ,s2.age
         |  FROM student_scores s1
         |  JOIN student_infos s2 ON s1.name = s2.name
         | WHERE s1.score >= 80
       """.stripMargin)
    goodStudentDF.printSchema()
    goodStudentDF.show()
    goodStudentDF.write.mode("overwrite").format("json").save("C:\\idea\\spark_demo\\src\\main\\scala\\com\\robin\\file\\good-students-scala")

  }
}
