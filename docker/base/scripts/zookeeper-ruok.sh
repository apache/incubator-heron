#!/usr/bin/env bash

# Check ZK server status

status=$(echo ruok | nc localhost 2181)
if [ "$status" == "imok" ]; then
  exit 0
else
  echo "ZK server is not ok"
  exit 1
fi
