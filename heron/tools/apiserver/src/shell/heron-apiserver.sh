#!/usr/bin/env bash

BINDIR=$(dirname $(readlink $0))
HERON_HOME=$(dirname ${BINDIR})
HERON_APISERVER_JAR=${HERON_HOME}/lib/api/heron-apiserver.jar
RELEASE_FILE=${HERON_HOME}/release.yaml

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

exec $JAVA -jar $HERON_APISERVER_JAR $@
