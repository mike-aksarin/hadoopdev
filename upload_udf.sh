#!/usr/bin/env bash
hdfs dfs -mkdir /jars/
hdfs dfs -put ./target/scala-2.11/hadoopdev_2.11-0.1.jar /jars/