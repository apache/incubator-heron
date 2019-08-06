#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

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
