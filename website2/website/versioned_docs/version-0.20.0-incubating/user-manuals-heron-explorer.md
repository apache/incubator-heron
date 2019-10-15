---
id: version-0.20.0-incubating-user-manuals-heron-explorer
title: Heron Explorer
sidebar_label: Heron Explorer
original_id: user-manuals-heron-explorer
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

The **Heron Explorer** is a CLI tool that you can use to gain insight into a Heron installation, including:

* which clusters are currently running in the installation
* information about a given topology's components (spouts and bolts)
* metrics for a topology
* the containers in which a topology is running
* the topologies running in a given cluster, role, or environment

> #### The Heron Explorer vs. Heron CLI
> There are two important differences between the Heron Explorer and [Heron CLI](user-manuals-heron-cli). Unlike Heron CLI, the Heron Explorer (a) requires the [Heron Tracker](user-manuals-heron-tracker-runbook) and (b) performs read-only, observation-oriented commands (rather than commands for actions like submitting, activating, and killing topologies).

In order to use the Heron Explorer, the [Heron Tracker](user-manuals-heron-tracker-runbook) will need to be running. If you've [installed the Tracker](getting-started-local-single-node), you can start it up using just one command:

```shell
$ heron-tracker
```

## Installation

The Heron Explorer is installed automatically if you follow the Heron tools installation tutorial in the [Quick Start Guide](getting-started-local-single-node#step-1-download-the-heron-tools).
x
## Commands

The commands available for the Heron Explorer are listed in the table below.

Command | Action | Arguments
:-------|:-------|:---------
[`clusters`](#clusters) | Lists all currently available Heron clusters | None
[`components`](#components) | Displays information about a topology's spout and bolt components, including each component's inputs and outputs (if any) and parallelism | `[cluster]/[role]/[env] [topology-name] [options]`
[`metrics`](#metrics) | Displays metrics for a topology | `[cluster]/[role]/[env] [topology-name] [options]`
[`containers`](#containers) | Displays all of the containers in which a topology is running | `[cluster]/[role]/[env] [topology-name] [options]`
[`topologies`](#topologies) | Displays all topologies currently running in the specified cluster, cluster/role, or cluster/role/env | `[cluster]/[role]/[env] [topology-name] [options]`


In addition to these commands, you can get help output by running `heron-explorer help` and the current version of Heron Explorer by running `heron-explorer version`.

## Cluster, role, and environment

To use a topology about which you'd like to gather information using Heron Explorer, you need to supply one of the following:

* the cluster name
* the cluster name and role
* the cluster name, role, and environment

Here are three examples corresponding to the options above:

```bash
$ heron-explorer topologies local
$ heron-explorer topologies us-west/finance
$ heron-explorer topologies asia-1/iot/devel
```

## Clusters

The `clusters` command lists all of the clusters running in the Heron installation. It takes no arguments.

Here's an example command and output:

```bash
$ heron-explorer clusters
Available clusters:
  local
```

## Components

The `components` command lists information about all of a topology's components (spouts and bolts), including:

* the type of the component (either `spout` or `bolt`)
* the name of the component
* the parallelism hint for the component
* the inputs and outputs for the component

Here's an example command and output:

```bash
$ heron-explorer components \
  us-east/analytics/staging \
  ClickCounterTopology
type    name             parallelism  input                  output
------  -------------  -------------  ---------------------  -------------
spout   click-ingest               2  -                      click-counter
bolt    click-counter              2 - click-ingest          persist-to-db
```

## Metrics

The `metrics` command lists a wide variety of metrics about each component in a topology:

* the container in which the component is running
* the JVM uptime for the component (in seconds)
* the JVM process CPU load for the component (as a %)
* the total JVM memory used by the component (in megabytes)
* the component's emit, ack, and fail counts

Here's an example command:

```bash
$ heron-explorer metrics \
  us-east/analytics/staging \
  ClickCounterTopology
```

And here's some example output:

```bash
'click-ingest' metrics:
container id                   jvm-uptime-secs    jvm-process-cpu-load    jvm-memory-used-mb    emit-count    ack-count    fail-count
--------------------------   -----------------  ----------------------  --------------------  ------------  -----------  ------------
container_1_click-ingest_1             1012                   0.510003                    83   2.81582e+07            0             0
container_2_click-ingest_2             1012                      0.467                    71   2.34582e+07            0             0

'click-counter' metrics:
container id                    jvm-uptime-secs    jvm-process-cpu-load    jvm-memory-used-mb    emit-count    ack-count    fail-count
----------------------------  -----------------  ----------------------  --------------------  ------------  -----------  ------------
container_1_click-counter_1                1012                  0.5201                    83   2.81432e+07            0             0
container_2_click-counter_2                1012                   0.481                    71   2.14896e+07            0             0
```

## Containers

The `containers` command lists all of the containers in which a topology is running, and provides the following information about each container:

* The ID of the container
* The host on which the container is running as well as the port it's using
* A process ID (pid) for the container
* The number of bolts, spouts, and total component instances

Here's an example command:

```bash
$ heron-explorer containers \
  us-east/analytics/staging \
  ClickCounterTopology
```

And here's some example output:

```bash
  container  host                  port    pid    #bolt    #spout    #instance
-----------  -----------------   ------  -----  -------  --------  -----------
          1  us-west.1.acme.com   62915  47893        1         1            1
          2  us-west.2.acme.com   62147  14390        1         1            1
```

## Topologies

The `topologies` command lists all topologies for one of the following:

* cluster
* cluster and role
* cluster, role, and environment

Here's an example command and output:

```bash
$ heron-explorer \
  us-east/analytics/staging
Topologies running in cluster 'us-east/analytics/staging'
role       env      topology
---------  -------  -------------------------
analytics  staging  ClickCounterTopology
```