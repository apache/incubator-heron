#!/bin/bash

set -e

DIR=`dirname $0`
source ${DIR}/common.sh

# check if the ci environ argument is provided
if [ $# -eq 0 ]; then
  echo "ci environ arg not provided (travis|applatix)"
fi

T="${DIR}/build.sh"
start_timer "$T"
${DIR}/build.sh $1
end_timer "$T"

T="make site"
start_timer "$T"
(cd website && make travis-site)
end_timer "$T"

T="${DIR}/test.sh"
start_timer "$T"
${DIR}/test.sh $1
end_timer "$T"

print_timer_summary
