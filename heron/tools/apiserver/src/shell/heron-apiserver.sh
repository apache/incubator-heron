#!/usr/bin/env bash

BINDIR=$(dirname $(readlink $0))
HERON_TOOLS_HOME=$(dirname ${BINDIR})
HERON_APISERVER_JAR=${HERON_TOOLS_HOME}/lib/heron-apiserver.jar
RELEASE_FILE=${HERON_TOOLS_HOME}/release.yaml

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


help() {
  cat <<EOF
usage: heron-apiserver <cluster>
  -D <property=value>                use value for given property
EOF
}

start_apiserver() {
  CLUSTER=$1; shift;
  CLUSTER_CONFIG_PATH=$HERON_TOOLS_HOME/conf/$CLUSTER

  exec $JAVA -jar $HERON_APISERVER_JAR \
    --cluster $CLUSTER \
    --config-path $CLUSTER_CONFIG_PATH \
    --release-file $RELEASE_FILE \
    $@
}

if [ "$#" -ge 1 ]; then
  start_apiserver $@
else
  help
  exit 1
fi
