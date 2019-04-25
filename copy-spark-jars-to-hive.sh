#!/usr/bin/env bash
cd /usr/local/spark/jars/
cp scala-library-2.11.12.jar /usr/local/hive/lib/
cp spark-core_2.11-2.4.0.jar /usr/local/hive/lib/
cp spark-network-common_2.11-2.4.0.jar /usr/local/hive/lib/
cp chill-java-0.9.3.jar /usr/local/hive/lib/
cp chill_2.11-0.9.3.jar /usr/local/hive/lib/
cp jackson-module-paranamer-2.7.9.jar /usr/local/hive/lib/
cp jackson-module-scala_2.11-2.6.7.1.jar /usr/local/hive/lib/
cp jersey-container-servlet-core-2.22.2.jar /usr/local/hive/lib/
cp jersey-server-2.22.2.jar /usr/local/hive/lib/
cp json4s-ast_2.11-3.5.3.jar /usr/local/hive/lib/
cp kryo-shaded-4.0.2.jar /usr/local/hive/lib/
cp minlog-1.3.0.jar /usr/local/hive/lib/
cp scala-xml_2.11-1.0.5.jar /usr/local/hive/lib/
cp spark-launcher_2.11-2.4.0.jar /usr/local/hive/lib/
cp spark-network-shuffle_2.11-2.4.0.jar /usr/local/hive/lib/
cp spark-unsafe_2.11-2.4.0.jar /usr/local/hive/lib/
cp xbean-asm6-shaded-4.8.jar /usr/local/hive/lib/

hadoop fs -copyFromLocal * /spark-jars

#cp conf/hive-site.xml /usr/local/hive/conf/