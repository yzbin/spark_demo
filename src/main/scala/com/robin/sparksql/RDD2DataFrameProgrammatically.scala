package com.robin.sparksql


import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with IntelliJ IDEA.
  * Description: 
  * User: bin
  * Date: 2017-12-21
  *
  */
object RDD2DataFrameProgrammatically extends App{
  System.setProperty("hadoop.home.dir","C:\\hadoop-2.7.0")
  val conf = new SparkConf()
    .setAppName("RDD2DataFrameProgrammatically")
    .setMaster("local[*]")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  // 第一步，构造出元素为Row的普通RDD
  val studentRowRDD = sc.textFile("C:\\idea\\spark_demo\\src\\main\\scala\\com\\robin\\file\\students.txt")
    .map{ line => Row(line.split(",")(0).toInt,line.split(",")(1),line.split(",")(2).toInt)}

  // 第二步，编程方式动态构造元数据
  val structType = StructType(
    Array(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true)
    )
  )

  // 第三步，进行RDD到DataFrame的转换
  val studentDF = sqlContext.createDataFrame(studentRowRDD, structType)
  studentDF.registerTempTable("students")
  val teenagerDF = sqlContext.sql(
    s"""
       |SELECT  id
       |       ,name
       |       ,age
       |  FROM students
     """.stripMargin)

  val teenagerRDD = teenagerDF.rdd
  teenagerRDD.foreach(row => println(row))

}
