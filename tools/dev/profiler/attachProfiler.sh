#!/bin/bash

yourkit_version=$1
service=$2
instance=$3

yourkit_port=`cat yourkit.port`
instance_pid=`cat ${instance}.pid`

echo Attaching profiler agent...
./${yourkit_version}/bin/yjp.sh -attach ${instance_pid} port=${yourkit_port}
echo Attached...
