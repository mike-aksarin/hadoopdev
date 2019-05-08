#!/usr/bin/env bash

#install postgresql for cloudera-quickstart docker image

yum install postgresql-server
service postgresql initdb
service postgresql start

cp postgresql-42.2.5.jre7.jar /usr/lib/sqoop/lib/

# change auth method from `indent` to `md5` in /var/lib/pgsql/data/pg_hba.conf
# run psql