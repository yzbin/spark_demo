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
object ParquetMergeSchema {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","C:\\hadoop-2.7.0")
    val conf = new SparkConf()
      .setAppName("ParquetMergeSchema")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    // 创建一个DataFrame，作为学生的基本信息，并写入一个parquet文件中
    val studentsWithNameAge = Array(
      ("leo", 23),
      ("jack", 25)
    ).toSeq
    val studentsWtihNameAgeDF = sc.parallelize(studentsWithNameAge, 2).toDF("name","age")
    studentsWtihNameAgeDF.write
      .mode("overwrite")
      .format("parquet")
      .save("C:\\idea\\spark_demo\\src\\main\\scala\\com\\robin\\file\\students")

    // 创建第二个DataFrame，作为学生的成绩信息，并写入一个parquet文件中
    val studentsWithNameGrade = Array(
      ("marry","A"),
      ("tom","B")
    ).toSeq
    val studentsWithNameGradeDF = sc.parallelize(studentsWithNameGrade, 2)toDF("name","grade")
    studentsWithNameGradeDF.write
      .mode("append")
      .format("parquet")
      .save("C:\\idea\\spark_demo\\src\\main\\scala\\com\\robin\\file\\students")

    val students = sqlContext.read.option("mergeSchema","true")
      .parquet("C:\\idea\\spark_demo\\src\\main\\scala\\com\\robin\\file\\students")

    students.printSchema()
    students.show()

  }
}
