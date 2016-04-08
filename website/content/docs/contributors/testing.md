---
title: Testing Heron
---

Heron uses [Bazel](../developers/compiling.html#installing-bazel) for building
and running unit tests. Before running tests, first set up your build environment
as described in [Compiling Heron](../../developers/compiling/compiling).

## Running Tests

The following command will run all tests:

```bash
$ bazel test --config=darwin heron/...
```

To run a specific [test
target](http://bazel.io/docs/test-encyclopedia.html), pass the test target name.

```bash
$ bazel test --config=darwin heron/statemgrs/tests/java:localfs-statemgr_unittest
```

## Discovering Test Targets

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
