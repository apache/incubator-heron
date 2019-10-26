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

LINK=$(readlink ${0})
if [ -z "$LINK" ]; then
   LINK=$0
fi
BINDIR=$(dirname ${LINK})
HERON_HOME=$(dirname ${BINDIR})
HERON_APISERVER_JAR=${HERON_HOME}/lib/api/heron-apiserver.jar
RELEASE_FILE=${HERON_TOOLS_HOME}/release.yaml
MEM_MIN=${HERON_APISERVER_MEM_MIN:-256M}
MEM_MAX=${HERON_APISERVER_MEM_MAX:-512M}

# Check for the java to use
if [[ -z $JAVA_HOME ]]; then
  JAVA=$(which java)
  if [ $? != 0 ]; then
   echo "Error: JAVA_HOME not set, and no java executable found in $PATH." 1>&2
   exit 1
  fi
else
  JAVA=${JAVA_HOME}/bin/java
fi

if [[ -n $HERON_APISERVER_MEM_DIRECT ]]; then
  OPTS="-XX:MaxDirectMemorySize=$HERON_APISERVER_MEM_DIRECT"
fi

exec $JAVA -jar -Xms${MEM_MIN} -Xmx${MEM_MAX} $OPTS $HERON_APISERVER_JAR $@
