<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->
# Heron CI scripts
This directory contains scripts used by CI jobs.

## Build release packages

Release packages include heron.tar.gz as well as installer script. Packages are platform dependent.

Example:

```bash
set -e
set -o pipefail

# Install bazel (linux build) because CI hosts may not have it installed
bash scripts/ci/setup_bazel.sh linux

# Build v0.20.1-incubating packages for rocky8 and put in artifacts folder
HERON_BUILD_USER=release-agent
bash scripts/ci/build_release_packages.sh v0.20.1-incubating rocky8 artifacts

```

## Build maven artifacts

Maven argifacts include api, spi, storm-compatibility and simulator. Artifacts are platform indepedent.

Example:

```bash
set -e
set -o pipefail

# Install bazel (linux build) because CI hosts may not have it installed
bash scripts/ci/setup_bazel.sh linux

# Build v0.20.1-incubating artifacts and put in artifacts folder
HERON_BUILD_USER=release-agent
bash scripts/ci/build_maven_artifacts.sh v0.20.1-incubating artifacts

```

## Build docker image

The docker image includes Heron core, tools and examples.

Example:

```bash
set -e
set -o pipefail

# Install bazel (linux build) because CI hosts may not have it installed
bash scripts/ci/setup_bazel.sh linux

# Build v0.20.1-incubating artifacts and put in artifacts folder
HERON_BUILD_USER=release-agent
bash scripts/ci/build_docker_image.sh v0.20.1-incubating 10 artifacts

```
