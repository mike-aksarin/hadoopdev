name := "hadoopdev"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.4.3"

val hiveVersion = "3.1.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.hive" % "hive-exec" % hiveVersion,
  "org.apache.logging.log4j" % "log4j-api" % "2.11.2",
  "com.opencsv" % "opencsv" % "4.5",
  "org.postgresql" % "postgresql" % "42.2.5",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test"
)