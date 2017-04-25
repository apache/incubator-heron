#!/bin/bash
# Copyright 2015 The Bazel Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Updates maven POM artifacts with version.

# Usage
# ./maven/maven-pom-version.sh VERSION
# Example
# ./maven/maven-pom-version.sh 0.14.1

if [ "$1" = "" ]; then
    echo "ERROR: heron version missing. Usage './maven/maven-pom-version.sh VERSION' "
    exit 1
fi

cat ./maven/heron-no-kryo.template.pom | \
    sed "s/VERSION/$1/g" | \
    sed "s/ARTIFACT_ID/heron-api/g" | \
    sed "s/NAME/heron-api/g" | \
    sed "s/DESCRIPTION/Heron API/g" \
    >> ./heron-api-$1.pom

cat ./maven/heron-no-kryo.template.pom | \
    sed "s/VERSION/$1/g" | \
    sed "s/ARTIFACT_ID/heron-spi/g" | \
    sed "s/NAME/heron-spi/g" | \
    sed "s/DESCRIPTION/Heron SPI/g" \
    >> ./heron-spi-$1.pom

cat ./maven/heron-with-kryo.template.pom | \
    sed "s/VERSION/$1/g" | \
    sed "s/ARTIFACT_ID/heron-storm/g" | \
    sed "s/NAME/heron-storm/g" | \
    sed "s/DESCRIPTION/Heron Storm/g" \
    >> ./heron-storm-$1.pom

