#!/usr/bin/env bash

ZOOSCRIPTS="${BASH_SOURCE-$0}"
ZOOSCRIPTS="$(dirname "${ZOOSCRIPTS}")"
ZOOSCRIPTS="$(cd "${ZOOSCRIPTS}"; pwd)"

ZKHOME="$(dirname "${ZOOSCRIPTS}")"
ZKCONFIG_FILE=$ZKHOME/conf/zookeeper.conf
ZKBIN=$ZKHOME/bin

# generate the zookeeper config based
$ZOOSCRIPTS/generate-zookeeper-config.sh $ZKCONFIG_FILE

export SERVER_JVMFLAGS="-Dnetworkaddress.cache.ttl=60"
# start zookeeper in the foreground with the generated config file
$ZKBIN/zkServer.sh start-foreground $ZKCONFIG_FILE
