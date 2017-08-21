#!/usr/bin/env bash

BINDIR=$(dirname "$0")
HERON_CORE=$(dirname ${BINDIR})
HERON_DOWNLOADER_JAR=${HERON_CORE}/lib/downloaders/heron-downloader.jar

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

exec $JAVA -jar $HERON_DOWNLOADER_JAR $@
