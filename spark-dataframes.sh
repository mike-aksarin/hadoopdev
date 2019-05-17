if sbt package
then

cp target/scala-2.11/hadoopdev_2.11-0.1.jar .

spark-submit \
--master local[*] \
--jars postgresql-42.2.5.jar \
--class SparkDataFrames \
hadoopdev_2.11-0.1.jar conf/application.conf

fi
