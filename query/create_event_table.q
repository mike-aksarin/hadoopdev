create external table events
(
  product_name string,
  product_price bigint,
  purchase_date timestamp,
  product_category string,
  client_ip string
)
partitioned by (date_tag string)
row format delimited
  fields terminated by ','
  --escaped by '"'
  collection items terminated by '|'
  lines terminated by '\n'
stored as textfile
location 'hdfs://localhost:9000/events/';
--tblproperties (
--    "hive.input.dir.recursive" = "TRUE",
--    "hive.mapred.supports.subdirectories" = "TRUE",
--    "hive.supports.subdirectories" = "TRUE",
--    "mapred.input.dir.recursive" = "TRUE"
--);