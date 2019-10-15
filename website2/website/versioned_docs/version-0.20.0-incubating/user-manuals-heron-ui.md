---
id: version-0.20.0-incubating-user-manuals-heron-ui-runbook
title: Heron UI Runbook
sidebar_label: Heron UI Runbook
original_id: user-manuals-heron-ui-runbook
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
* `--address` - Address to listen; Default address is `0.0.0.0`
* `--base_url` - The base url path if operating behind proxy; Default is [`None`](https://github.com/apache/incubator-heron/blob/master/heron/tools/ui/src/python/main.py#L145)

```bash
$ heron-ui
# is equivalent to
$ heron-ui --port=8889 --tracker_url=http://localhost:8888
```
