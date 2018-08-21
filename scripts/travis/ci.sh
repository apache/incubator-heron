#!/bin/bash
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

set -e

DIR=`dirname $0`
UTILS=${DIR}/../shutils
source ${UTILS}/common.sh

T="${DIR}/build.sh"
start_timer "$T"
${DIR}/build.sh
end_timer "$T"

${DIR}/check.sh

#T="make site"
#start_timer "$T"
#(cd website && make travis-site)
#end_timer "$T"

if [ -z ${DISABLE_INTEGRATION_TESTS+x} ]; then
  T="${DIR}/test.sh"
  start_timer "$T"
  ${DIR}/test.sh
  end_timer "$T"
else
  echo "bypass integration tests"
fi

print_timer_summary
