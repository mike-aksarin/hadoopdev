#cat query/init_metastore.sql > sudo -u postgres psql

schematool -dbType postgres -initSchema

schematool -dbType postgres -info

# start hive metastore server -hiveconf hive.root.logger=DEBUG,console
# hive --service metastore
# ps -ef |grep -i metastore

# debug hive
# hive -hiveconf hive.root.logger=DEBUG,console