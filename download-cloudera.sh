#!/usr/bin/env bash

#sudo apt-get install docker.io

docker pull cloudera/quickstart:latest

#docker run --name cdq --hostname=quickstart.cloudera --privileged=true -t -i -P -p 8888:8888 -p 7180:7180 -p 80:80 -p 8088:8088 -p 4040:4040 -p 4041:4041 -p 8082:8080 -p 8081:8081 cloudera/quickstart /usr/bin/docker-quickstart

#docker ps

#docker port cdh [GUEST PORT]

# /home/cloudera/cloudera-manager

# copy local file to cdh

# docker cp conf/flume.properties cdq:/home/hadoopdev/conf/

# exec flume
#docker exec -t cdq /home/hadoopdev/flume-loader.sh

#exec event-source
# docker exec -t cdq /home/hadoopdev/event-source.sh