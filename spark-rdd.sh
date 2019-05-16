if sbt package
then

cp target/scala-2.11/hadoopdev_2.11-0.1.jar .
#cp ~/.ivy2/cache/com.opencsv/opencsv/jars/opencsv-4.5.jar .

spark-submit \
--master local[*] \
--jars opencsv-4.5.jar,postgresql-42.2.5.jar \
--class SparkRdd \
hadoopdev_2.11-0.1.jar conf/application.conf

fi
