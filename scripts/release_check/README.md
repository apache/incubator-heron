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
# Release Check Scripts

These are the convenience scripts for verifying a release.

Currently the scripts work on MacOS.

Development environment setup is required. Setup instrctuction can be found here: https://apache.github.io/incubator-heron/docs/developers/compiling/mac/.

## Run all release checks
```
sh ./scripts/release_check/full_release_check.sh [PATH_TO_RAT_JAR_FILE]
```

## Run individual release checks

### To run a license check with Apache Rat. Apache Rat can be downloaded here: http://ftp.wayne.edu/apache/creadur/apache-rat-0.13/apache-rat-0.13-bin.tar.gz. Decompress it if needed.
```
sh ./scripts/release_check/license_check.sh [PATH_TO_RAT_JAR_FILE]
```

### To compile source, into Heron release artifacts (MacOS).
```
sh ./scripts/release_check/build.sh
```

### To run a test topology locally (after build.sh is executed).
```
sh ./scripts/release_check/run_test_topology.sh
```

### To compile source into a Heron docker image (host OS: MacOS, target OS: debian11).
```
sh ./scripts/release_check/build_docker.sh
```
