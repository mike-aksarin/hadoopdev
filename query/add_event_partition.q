alter table events add partition(date_tag = "2019-04-22")
location 'hdfs://localhost:9000/events/2019/04/22';