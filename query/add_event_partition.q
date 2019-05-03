alter table events add partition(date_tag = "2019-04-29")
 location 'hdfs://localhost:9000/events/2019/04/29';

--alter table events add partition(date_tag = "2019-05-01")
--location 'hdfs://quickstart.cloudera:8020/events/2019/05/01';