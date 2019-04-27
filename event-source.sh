#!/usr/bin/env bash

cd `dirname $0`

source ./env.sh

echo "Running event source at port $NC_PORT"

sbt run | nc localhost $NC_PORT

