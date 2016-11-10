---
title: Testing Heron
---

Heron uses [Bazel](../../developers/compiling#installing-bazel) for building
and running unit tests. Before running tests, first set up your build environment
as described in [Compiling Heron](../../developers/compiling/compiling).

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
    $ bazel build --config=darwin integration-test/src/...
    $ ./bazel-bin/integration-test/src/python/local_test_runner/local-test-runner
    ```
