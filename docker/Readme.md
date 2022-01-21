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
# Docker

## To compile source, into Heron release artifacts, using a docker container:
Make sure enough resources are configured in Docker settings: 2 CPU, 4G RAM and 128G disk.
```
./docker/scripts/build-artifacts.sh <platform> <version_string> [source-tarball] <output-directory>
# e.g.  ./docker/scripts/build-artifacts.sh ubuntu20.04 testbuild ~/heron-release
```

## To build docker containers for running heron daemons:
```
./docker/scripts/build-docker.sh <platform> <version_string> <output-directory>
# e.g. ./docker/scripts/build-docker.sh ubuntu20.04 testbuild ~/heron-release
```
