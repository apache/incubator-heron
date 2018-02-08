#!/usr/bin/env bash

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <zookeeper-host> <zookeeper-port>"
  exit 1
fi

WAIT_ZK_HOST=$1
WAIT_ZK_PORT=$2

while true; do
  status=$(echo ruok | nc $WAIT_ZK_HOST $WAIT_ZK_PORT);
  writestatus=$(echo isro | nc $WAIT_ZK_HOST $WAIT_ZK_PORT)
  if [ "$status" = "imok" ] && [ "$writestatus" == "rw" ]; then
    echo "Zookeeper $WAIT_ZK_HOST:$WAIT_ZK_PORT is ready";
    exit 0
  fi;
  echo "Zookeeper $WAIT_ZK_HOST:$WAIT_ZK_PORT not ready";
  sleep 4;
done
