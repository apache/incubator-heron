---
id: user-manuals-heron-ui-runbook
title: Heron UI Runbook
sidebar_label: Heron UI Runbook
---

**Heron UI** is a user interface that uses the [Heron Tracker](heron-architecture#heron-tracker) to display detailed,
colorful visual representations of topologies, including the
[logical](heron-topology-concepts#logical-plan) and [physical
plan](heron-topology-concepts#physical-plan) for each topology. Check out
[Heron UI Usage Guide](guides-ui-guide) for more information about
various elements that UI exposes.

### Building Heron UI

Heron uses [bazel](http://bazel.io/) for compiling.
[This page](compiling-overview) describes how to setup bazel
for heron.

```bash
# Build heron-ui
$ bazel build heron/tools/ui/src/python:heron-ui

# The location of heron-ui pex executable is
# bazel-bin/heron/tools/ui/src/python/heron-ui
# To run using default options:
$ ./bazel-bin/heron/tools/ui/src/python/heron-ui
```

`heron-ui` is a self executable
[pex](https://pex.readthedocs.io/en/latest/whatispex.html) archive.

### Heron UI Args

* `--port` - Port to run the heron-ui on. Default port is `8889`.
* `--tracker_url` - The base url for tracker. All the information about the
  topologies is fetched from tracker. Default url is `http://localhost:8888`.

```bash
$ heron-ui
# is equivalent to
$ heron-ui --port=8889 --tracker_url=http://localhost:8888
```
