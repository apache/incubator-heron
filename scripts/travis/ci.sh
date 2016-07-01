#!/bin/bash

set -e

DIR=`dirname $0`

source ${DIR}/common.sh

T="${DIR}/build.sh"
start_timer "$T"
${DIR}/build.sh
end_timer "$T"

T="make site"
start_timer "$T"
(cd website && make travis-site)
end_timer "$T"

T="${DIR}/test.sh"
start_timer "$T"
${DIR}/test.sh
end_timer "$T"

print_timer_summary
