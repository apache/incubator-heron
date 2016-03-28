---
title: Testing Heron
---

You can run unit tests for Heron using
[Bazel](../developers/compiling.html#installing-bazel).

To run the test for a given [test
target](http://bazel.io/docs/test-encyclopedia.html), run `bazel test
path/to/target:target_name` and make sure that you specify an OS-specific
configuration using the `--config` flag. This can take one of the following
values: `darwin` (Mac OS X), `ubuntu` (Ubuntu 12.04), `centos5` (CentOS 5).

Here's an example test command:

```bash
$ bazel test --config=darwin heron/state/tests/java:local_file_state_manager_unittest
```

Here's an example to run all tests:

```bash
$ bazel test --config=darwin heron/...
```

### Discovering Test Targets

To fetch a full listing of all Bazel test targets:

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
