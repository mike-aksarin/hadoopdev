create user hive WITH PASSWORD 'hive';

create database hive_metastore;

grant all on database hive_metastore to hive;