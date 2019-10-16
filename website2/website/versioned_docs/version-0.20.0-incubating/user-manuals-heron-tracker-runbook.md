---
id: version-0.20.0-incubating-user-manuals-heron-tracker-runbook
title: Heron Tracker
sidebar_label: Heron Tracker Runbook
original_id: user-manuals-heron-tracker-runbook
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

**Heron Tracker** is a web service that continuously gathers a wide
variety of information about Heron topologies and exposes
that information through a [JSON REST API](user-manuals-tracker-rest).
More on the role of the Tracker can be found
[here](heron-architecture#heron-tracker).

## Building Heron Tracker

Heron uses [bazel](http://bazel.io/) for compiling.
[Compiling](compiling-overview) describes how to setup bazel
for heron.

```bash
# Build heron-tracker
$ bazel build heron/tools/tracker/src/python:heron-tracker

# The location of heron-tracker pex executable is
# bazel-bin/heron/tools/tracker/src/python/heron-tracker
# To run using default options:
$ ./bazel-bin/heron/tools/tracker/src/python/heron-tracker
```

`heron-tracker` is a self executable
[pex](https://pex.readthedocs.io/en/latest/whatispex.html) archive.

### Heron Tracker Config File

The config file is a `yaml` file that should contain the following information.

#### 1. State Manager locations

This is a list of locations where topology writes its states. An example of
[zookeeper state manager](state-managers-zookeeper) and
[local file state manager](state-managers-local-fs) look like this:

```yaml
## Contains the sources where the states are stored.
# Each source has these attributes:
# 1. type - type of state manager (zookeeper or file, etc.)
# 2. name - name to be used for this source
# 3. hostport - only used to connect to zk, must be of the form 'host:port'
# 4. rootpath - where all the states are stored
# 5. tunnelhost - if ssh tunneling needs to be established to connect to it
statemgrs:
  -
    type: "file"
    name: "local"
    rootpath: "~/.herondata/repository/state/local"
    tunnelhost: "localhost"
#
# To use 'localzk', launch a zookeeper server locally
# and create the following path:
#  *. /heron/topologies
#
#  -
#    type: "zookeeper"
#    name: "localzk"
#    hostport: "localhost:2181"
#    rootpath: "/heron"
#    tunnelhost: "localhost" -
```

Topologies from all the state managers would be read and can be queried from
Tracker.

Note that Tracker does not create any zookeeper nodes itself. It is a readonly
service. If you launch the Tracker without creating `/topologies` node under the
rootpath, Tracker will fail with a `NoNodeError`.

#### 2. Viz URL Format

This is an optional config. If it is present, then it will show up for each
topology as the viz link as shown below. For each topology, these parameters
will be filled appropriately. This parameter can be used to link metrics
dashboards with topology UI page.

![Viz Link](assets/viz-link.png)

```yaml
# The URL that points to a topology's metrics dashboard.
# This value can use following parameters to create a valid
# URL based on the topology. All parameters are self-explanatory.
# These are found in the execution state of the topology.
#
#   ${CLUSTER}
#   ${ENVIRON}
#   ${TOPOLOGY}
#   ${ROLE}
#   ${USER}
#
# This is a sample, and should be changed to point to corresponding dashboard.
#
# viz.url.format: "http://localhost/${CLUSTER}/${ENVIRON}/${TOPOLOGY}/${ROLE}/${USER}"
```

### Heron Tracker Args

* `--port` - Port to run the heron-tracker on. Default port is `8888`.
* `--config-file` - The location of the config file for tracker. Default config
  file is `~/.herontools/conf/heron_tracker.yaml`.

```bash
$ heron-tracker
# is equivalent to
$ heron-ui --port=8888 --config-file=~/.herontools/conf/heron_tracker.yaml
```
