#!/usr/bin/env bash

cp ./target/scala-2.11/hadoopdev_2.11-0.1.jar .
cp ~/.ivy2/cache/org.scala-lang/scala-library/jars/scala-library-2.11.8.jar .
cp ~/.ivy2/cache/org.scala-lang/scala-reflect/jars/scala-reflect-2.11.8.jar .

# docker cp ./target/scala-2.11/hadoopdev_2.11-0.1.jar cdq:/home/hadoopdev/
# docker cp ~/.ivy2/cache/org.scala-lang/scala-library/jars/scala-library-2.11.8.jar cdq:/home/hadoopdev/
# docker cp ~/.ivy2/cache/org.scala-lang/scala-reflect/jars/scala-reflect-2.11.8.jar cdq:/home/hadoopdev/

# hdfs dfs -mkdir /jars/
# hdfs dfs -put -f ./target/scala-2.11/hadoopdev_2.11-0.1.jar /jars/
