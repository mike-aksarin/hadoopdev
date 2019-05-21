# Hadoop Ecosystem Example

This is a final task for a Hadoop Developers gridU course.

## 1. Deploy CDH cluster

Deploy quickstart ClouderaVM.

Check that you have all this services installed:

* HDFS
* Hive
* Flume
* Sqoop
* YARN
* Zookeeper

## 2. Implement Random Events producer

Use Java, Scala or Python to implement event producer. Each event message describes single product purchase.

Producer should connect to Flume socket (see below) and send events in a CSV format, one event per line.

Product purchase event properties:

| property         | distibution type requirement   | data requirement |
|------------------|--------------------------------|------------------|
| product name     | uniform                        | 
| product price    | gaussian                       | 
| purchase date    | time - gaussian date - uniform | 1 week range     |
| product category | uniform                        |
| client IP address| uniform                        | IPv4 random adresses

#### Note
Hive CSV SerDe uses OpenCSV library, so you could try it as well.

## 3. Configure Flume to consume events using NetCat Source

Flume should put events to HDFS directory `events/${year}/${month}/${day}`

Try to put more than 3000 events to HDFS in several batches

## 4. Create external Hive table to process data

External table should be partitioned by one field - `purchase date`. All partitions should be created manually right after you apply Hive scheme.

#### Note
HDFS data structure and partitions in Hive table are not the same.

E.g. HDFS location = `events/2016/02/14`, Hive partition = `2016-02-14`

## 5. Execute complex select queries

* Select top 10  most frequently purchased categories

* Select top 10 most frequently purchased product in each category

## 6. JOIN events with geodata
Usage of https://www.maxmind.com/en/geoip2-country-database is prohibited. 

* Put data from http://dev.maxmind.com/geoip/geoip2/geolite2/ to HIVE table

* JOIN events data with ip geo data

* Select top 10 countries with the highest money spending

####Note

Hive UDF might help you to join events with IP geo data

## 7. Put Hive queries result to RDBMS via Sqoop
* Install any RDBMS (e.g. MySQL, PostgreSQL)
* Install and configure Sqoop
* Put queries result from step 5 and 6 result to RDBMS. (Write down RDBMS tables creation scripts and Sqoop commands to export data)

##8. Spark Core

####Note

Use Scala for all spark connected code. It would be great to implement solution both in terms of RDDs and Datasets.

### Spark Practice

Receive the same result as in steps 5-7 using Apache Spark instead of Hive.

####Note

Spark Streaming is not required in this task. Hive is not required as well. Spark should read and process CSV files from HDFS.

Spark is able to export data directly to RDBMS. So you don't need Sqoop anymore. Now your flow should look like 
```
Producer -> Flume -> HDFS -> Spark -> RDBMS
```

Execute and debug spark in your IDE

### Compare results 
Hive queries results should match Spark output.

