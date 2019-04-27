-- alter table events add partition(date_tag = "2019-04-22")
-- location 'hdfs://localhost:9000/events/2019/04/22';

alter table events add partition(date_tag = "2019-04-27")
location 'hdfs://quickstart.cloudera:8020/events//2019/04/27';