#!/usr/bin/env bash

#sudo apt-get install docker.io

docker pull cloudera/quickstart:latest

#docker run --name cdh --hostname=quickstart.cloudera --privileged=true -t -i -P -p 4040:4040 -p 4041:4041 -p 8080:8080 -p 8081:8081 cloudera/quickstart /usr/bin/docker-quickstart

#docker ps

#docker port [CONTAINER HASH] [GUEST PORT]

# /home/cloudera/cloudera-manager

# copy local file to cdh

# docker cp conf/flume.properties cdh:/home/hadoopdev/conf/

# exec flume
#docker exec -t cdh /home/hadoopdev/flume-loader.sh

#exec event-source
# docker exec -t cdh /home/hadoopdev/event-source.sh