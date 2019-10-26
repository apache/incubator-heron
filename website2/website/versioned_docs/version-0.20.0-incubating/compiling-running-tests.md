---
id: version-0.20.0-incubating-compiling-running-tests
title: Running Tests
sidebar_label: Running Tests
original_id: compiling-running-tests
---
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

Heron uses [Bazel](compiling-overview#installing-bazel) for building
and running unit tests. Before running tests, first set up your build environment
as described in [Compiling Heron](compiling-overview).

### Running Unit Tests

The following command will run all tests:

```bash
$ bazel test --config=darwin heron/...
```

To run a specific [test
target](http://bazel.io/docs/test-encyclopedia.html), pass the test target name.

```bash
$ bazel test --config=darwin heron/statemgrs/tests/java:localfs-statemgr_unittest
```

### Discovering Unit Test Targets

To see a full listing of all Bazel test targets:

```bash
$ bazel query 'kind(".*_test rule", ...)'
```

For **Java** targets only:

```bash
$ bazel query 'kind("java_test rule", ...)'
```

For **C++** targets:

```bash
$ bazel query 'kind("cc_test rule", ...)'
```

For **Python** targets:

```bash
$ bazel query 'kind("pex_test rule", ...)'
```

### Running Integration Tests

Integration tests are divided into two categories:

* Functional integration tests

    These integration tests are designed for testing the functionality of 
    Heron, such as topologies and groupings.
    To run the functional integration tests on a Mac OS X, do the following:

    ```bash
    $ ./scripts/run_integration_test.sh
    ```

* Failure integration tests

    These integration tests are designed for testing recovery from failure/restart
    in certain processes, such as Topology Master and Metrics Manager.
    To run the failure integration tests on a Mac OS X, do the following:

    ```bash
    $ bazel build --config=darwin integration_test/src/...
    $ ./bazel-bin/integration_test/src/python/local_test_runner/local-test-runner
    ```
