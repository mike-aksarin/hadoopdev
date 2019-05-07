ADD JAR hadoopdev_2.11-0.1.jar;
ADD JAR scala-library-2.11.8.jar;
ADD JAR scala-reflect-2.11.8.jar;

CREATE FUNCTION match_network AS 'MatchNetworkFunction';

-- USING JAR 'hdfs://quickstart.cloudera:8020/jars/hadoopdev_2.11-0.1.jar';
