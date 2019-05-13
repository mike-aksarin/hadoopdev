sbt package

cp target/scala-2.11/hadoopdev_2.11-0.1.jar ./

spark-submit \
--master local[*] \
--class SparkRdd \
hadoopdev_2.11-0.1.jar
