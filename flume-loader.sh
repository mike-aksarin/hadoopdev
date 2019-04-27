#!/usr/bin/env bash

cd `dirname $0`

source ./env.sh

echo "Running flume loader at port $NC_PORT"

flume-ng agent -n loader -f conf/flume.properties --conf conf -Dflume.root.logger=INFO,console -DpropertiesImplementation=org.apache.flume.node.EnvVarResolverProperties