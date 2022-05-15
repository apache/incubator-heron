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
This directory (integration_test) contains all the source code and scripts for the
integration test framework and tests.

The main components are:

 * Test framework (`src/java/.../(core, common)`)
   - this forms the core framework which ensures the output of the topology is
     converted to json, and uploaded to the test server. It also provides helper
     spouts and bolts which are commonly used by tests.

 * Test topologies (`src/java/.../topology`)
   - Test topologies make use of the above framework library to test heron functionality.
     Only this section needs to be touched to add any new tests.

 * Test runner (`src/python/test_runner`)
   - Test runner picks up the test topologies, launches them on aurora, polls for the actual test
     results (from the http server), compares that with expected results and kills the topology.

 * Test http server (`src/python/http_server`)
   - a simple http server (launched as an aurora job) to host the integration test results
     Test topologies `post` the result, and test runner `get`s the result.

 * Local test runner (`src/python/local_test_runner`)
   - Local test runner launches topologies locally, and polls for the actual results (from the
     local file system).

To run integration tests on your mac first run the following to build the test package and install
the heron client:

```Bash
  bazel build integration_test/src/...
  bazel run -- scripts/packages:heron-client-install.sh --user
  bazel run -- scripts/packages:heron-tools-install.sh --user
```

To run the local integration tests on your mac run the following from the heron repo's top dir:

```Bash
  ./bazel-bin/integration_test/src/python/local_test_runner/local-test-runner
```

To run just a single test include the module and test class:
```Bash
  ./bazel-bin/integration_test/src/python/local_test_runner/local-test-runner test_template.TestTemplate
```
