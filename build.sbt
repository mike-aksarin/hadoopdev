name := "hadoopdev"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.4.0"

val hiveVersion = "3.1.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.hive" % "hive-exec" % hiveVersion,
  "com.opencsv" % "opencsv" % "4.5",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test"
)