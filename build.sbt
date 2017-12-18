name := "spark_demo"

version := "0.1"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-api" % "1.7.21",
  "org.slf4j" % "slf4j-log4j12" % "1.7.21",
  "org.scala-lang" % "scala-library" % "2.10.6",
  "org.scala-lang" % "scala-reflect" % "2.10.6",
  "org.scala-lang" % "scala-compiler" % "2.10.6",
  "org.apache.spark" % "spark-core_2.10" % "1.6.3",
  "org.apache.spark" % "spark-sql_2.10" % "1.6.3",
  "org.apache.spark" % "spark-mllib_2.10" % "1.6.3",
  "org.apache.spark" % "spark-streaming_2.10" % "1.6.3",
  "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.6.3",
  "org.apache.spark" % "spark-hive_2.10" % "1.6.3",
  "com.yammer.metrics" % "metrics-core" % "2.2.0",
  "org.apache.commons" % "commons-email" % "1.4",
  "javax.mail" % "mail" % "1.4.7",
  "redis.clients" % "jedis" % "2.7.2"
)
        