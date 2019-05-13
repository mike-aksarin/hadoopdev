cd `dirname $0`

source ./env.sh

echo "Running event source at port $NC_PORT"

sbt compile

sbt -error "runMain EventSource" | nc localhost $NC_PORT
