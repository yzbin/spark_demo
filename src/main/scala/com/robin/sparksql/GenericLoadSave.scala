package com.robin.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with IntelliJ IDEA.
  * Description: 
  * User: bin
  * Date: 2017-12-20
  *
  */
object GenericLoadSave {
  case class sort(a:String,b:String)
  def main(args: Array[String]): Unit = {
    def test1():Unit={
      System.setProperty("hadoop.home.dir","C:\\hadoop-2.7.0")
      val conf = new SparkConf()
        .setAppName("GenericLoadSave")
        .setMaster("local")
      val sc = new SparkContext(conf)
      val sqlContext = new SQLContext(sc)
      val rdd: RDD[String] = sc.textFile("C:\\idea\\spark_demo\\src\\main\\scala\\com\\robin\\file\\sort.txt")

      import sqlContext.implicits._
      rdd.map(e=>{
        val arr = e.split("\t")
        sort(arr.apply(0),arr.apply(1))
      }).toDF().write.format("parquet").save("C:\\idea\\spark_demo\\src\\main\\scala\\com\\robin\\file\\cc.parquet")

      val users2DF = sqlContext.read.load("C:\\idea\\spark_demo\\src\\main\\scala\\com\\robin\\file\\cc.parquet")
      users2DF.show()
    }

    def test2():Unit = {
      System.setProperty("hadoop.home.dir","C:\\hadoop-2.7.0")
      val conf = new SparkConf()
        .setAppName("GenericLoadSave")
        .setMaster("local[*]")
      val sc = new SparkContext(conf)
      val sqlContext = new SQLContext(sc)

      import sqlContext.implicits._
      val user2DF = sqlContext.read.load("C:\\idea\\spark_demo\\src\\main\\scala\\com\\robin\\file\\users.parquet")
      user2DF.show()
      user2DF.write.mode("overwrite").save("C:\\idea\\spark_demo\\src\\main\\scala\\com\\robin\\file\\namesAndFavColors_scala")
    }

//    test1()
   test2()
  }
}
