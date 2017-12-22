package com.robin.sparksql

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with IntelliJ IDEA.
  * Description: 
  * User: bin
  * Date: 2017-12-22
  *
  */
object HiveDataSource {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("HiveDataSource")
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)

    // 删除表
    hiveContext.sql(
      s"""
         |DROP TABLE IF EXISTS student_infos
       """.stripMargin)

    // 创建表
    hiveContext.sql(
      s"""
         |CREATE TABLE IF NOT EXISTS student_infos (name STRING, age INT)
       """.stripMargin)

    hiveContext.sql(
      s"""
         |LOAD DATA LOCAL INPATH '/home/jflm/etl/test/spark_test/test_file/student_infos.txt' OVERWRITE INTO TABLE student_infos
       """.stripMargin)

    hiveContext.sql(
      s"""
         |DROP TABLE IF EXISTS student_scores
       """.stripMargin)
    hiveContext.sql(
      s"""
         |CREATE TABLE IF NOT EXISTS student_scores (name STRING , score INT)
       """.stripMargin)
    hiveContext.sql(
      s"""
         |LOAD DATA LOCAL INPATH '/home/jflm/etl/test/spark_test/test_file/student_scores.txt' OVERWRITE INTO TABLE student_scores
       """.stripMargin)

    val goodStudentDF = hiveContext.sql(
      s"""
         |SELECT  s1.name
         |       ,s1.age
         |       ,s2.score
         |  FROM student_infos s1
         |  JOIN student_scores s2 ON s1.name = s2.name
         | WHERE s2.score >= 80
       """.stripMargin)

    goodStudentDF.write
      .mode("overwrite")
      .saveAsTable("good_student_info")

    val goodStudents = hiveContext.table("good_student_info").collect()

    for (goodStudentRow <- goodStudents){
      println(goodStudentRow)
    }

  }

}
