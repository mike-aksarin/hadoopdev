CREATE FUNCTION match_network AS 'MatchNetworkFunction'
  USING JAR 'hdfs://localhost:9000/jars/hadoopdev_2.11-0.1.jar';
  -- USING JAR 'hdfs://quickstart.cloudera:8020/jars/hadoopdev_2.11-0.1.jar';
