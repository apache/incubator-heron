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

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 zookeeper-<version-number>"
  echo "Example:"
  echo "$ $0 zookeeper-3.8.0"
  exit 1
fi

ZK_DIST=$1

curl -O "https://archive.apache.org/dist/zookeeper/$ZK_DIST/apache-$ZK_DIST-bin.tar.gz"
tar -xzf ./apache-$ZK_DIST-bin.tar.gz -C /opt
rm ./apache-$ZK_DIST-bin.tar.gz

mv /opt/apache-$ZK_DIST-bin /opt/zookeeper
rm -rf \
  /opt/zookeeper/README.txt \
  /opt/zookeeper/NOTICE.txt \
  /opt/zookeeper/README_packaging.txt \
  /opt/zookeeper/docs \


# copy zk scripts
mkdir -p /opt/zookeeper/scripts
cp /opt/heron-docker/scripts/generate-zookeeper-config.sh /opt/zookeeper/scripts/
chmod +x /opt/zookeeper/scripts/generate-zookeeper-config.sh
cp /opt/heron-docker/scripts/zookeeper-ruok.sh /opt/zookeeper/scripts/
chmod +x /opt/zookeeper/scripts/zookeeper-ruok.sh
cp /opt/heron-docker/scripts/start-zookeeper.sh /opt/zookeeper/scripts/
chmod +x /opt/zookeeper/scripts/start-zookeeper.sh
cp /opt/heron-docker/scripts/wait-for-zookeeper.sh /opt/zookeeper/scripts/
chmod +x /opt/zookeeper/scripts/wait-for-zookeeper.sh