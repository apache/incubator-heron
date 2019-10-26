---
id: version-0.20.0-incubating-compiling-code-organization
title: Code Organization
sidebar_label: Code Organization
original_id: compiling-code-organization
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

This document contains information about the Heron codebase intended primarily
for developers who want to contribute to Heron. The Heron codebase lives on
[github]({{% githubMaster %}}).

If you're looking for documentation about developing topologies for a Heron
cluster, see [Building Topologies](topology-development-topology-api-java) instead.

## Languages

The primary programming languages for Heron are C++, Java, and Python.

* **C++ 11** is used for most of Heron's core components, including the
[Topology Master](heron-architecture#topology-master), and
[Stream Manager](heron-architecture#stream-manager).

* **Java 8** is used primarily for Heron's [topology
API](heron-topology-concepts), and [Heron Instance](heron-architecture#heron-instance).
It is currently the only language in which topologies can be written. Instructions can be found
in [Building Topologies](../../developers/java/topologies), while documentation for the Java
API can be found [here](/api/org/apache/heron/api/topology/package-summary.html). Please note that Heron topologies do not
require Java 8 and can be written in Java 7 or later.

* **Python 2** (specifically 2.7) is used primarily for Heron's [CLI interface](user-manuals-heron-cli) and UI components such as [Heron UI](user-manuals-heron-ui) and the [Heron Tracker](user-manuals-heron-tracker-runbook).

## Main Tools

* **Build tool** --- Heron uses [Bazel](http://bazel.io/) as its build tool.
Information on setting up and using Bazel for Heron can be found in [Compiling Heron](compiling-overview).

* **Inter-component communication** --- Heron uses [Protocol
Buffers](https://developers.google.com/protocol-buffers/?hl=en) for
communication between components. Most `.proto` definition files can be found in
[`heron/proto`]({{% githubMaster %}}/heron/proto).

* **Cluster coordination** --- Heron relies heavily on ZooKeeper for cluster
coordination for distributed deployment, be it for [Aurora](schedulers-aurora-cluster) or for a [custom
scheduler](extending-heron-scheduler) that you build. More information on ZooKeeper
components in the codebase can be found in the [State
Management](#state-management) section below.

## Common Utilities

The [`heron/common`]({{% githubMaster %}}/heron/common) contains a variety of
utilities for each of Heron's languages, including useful constants, file
utilities, networking interfaces, and more.

## Cluster Scheduling

Heron supports two cluster schedulers out of the box:
[Aurora](schedulers-aurora-cluster) and a [local
scheduler](schedulers-local). The Java code for each of those
schedulers can be found in [`heron/schedulers`]({{% githubMaster %}}/heron/schedulers)
, while the underlying scheduler API can be found [here](/api/org/apache/heron/spi/scheduler/package-summary.html)

Info on custom schedulers can be found in [Implementing a Custom
Scheduler](extending-heron-scheduler); info on the currently available schedulers
can be found in [Deploying Heron on
Aurora](schedulers-aurora-cluster) and [Local
Deployment](schedulers-local).

## State Management

The parts of Heron's codebase related to
[ZooKeeper](http://zookeeper.apache.org/) are mostly contained in
[`heron/state`]({{% githubMaster %}}/heron/state). There are ZooKeeper-facing
interfaces for [C++]({{% githubMaster %}}/heron/state/src/cpp),
[Java]({{% githubMaster %}}/heron/state/src/java), and
[Python]({{% githubMaster %}}/heron/state/src/python) that are used in a variety of
Heron components.

## Topology Components

### Topology Master

The C++ code for Heron's [Topology
Master](heron-architecture#topology-master) is written in C++ can be
found in [`heron/tmaster`]({{% githubMaster %}}/heron/tmaster).

### Stream Manager

The C++ code for Heron's [Stream
Manager](heron-architecture#stream-manager) can be found in
[`heron/stmgr`]({{% githubMaster %}}/heron/stmgr).

### Heron Instance

The Java code for [Heron
instances](heron-architecture#heron-instance) can be found in
[`heron/instance`]({{% githubMaster %}}/heron/instance).

### Metrics Manager

The Java code for Heron's [Metrics
Manager](heron-architecture#metrics-manager) can be found in
[`heron/metricsmgr`]({{% githubMaster %}}/heron/metricsmgr).

If you'd like to implement your own custom metrics handler (known as a **metrics
sink**), see [Implementing a Custom Metrics Sink](extending-heron-metric-sink).

## Developer APIs

### Topology API

Heron's API for writing topologies is written in Java. The code for this API can
be found in [`heron/api`]({{% githubMaster %}}/heron/api).

Documentation for writing topologies can be found in [Building
Topologies](topology-development-topology-api-java), while API documentation can be found
[here](/api/org/apache/heron/api/topology/package-summary.html).

### Simulator

Heron enables you to run topologies in [`Simulator`](guides-simulator-mode)
for debugging purposes.

The Java API for simulator can be found in
[`heron/simulator`](/api/org/apache/heron/simulator/package-summary.html).

### Example Topologies

Heron's codebase includes a wide variety of example
[topologies](heron-topology-concepts) built using Heron's topology API for
Java. Those examples can be found in
[`heron/examples`]({{% githubMaster %}}/heron/examples).

## User Interface Components

### Heron CLI

Heron has a tool called `heron` that is used to both provide a CLI interface
for [managing topologies](user-manuals-heron-cli) and to perform much of
the heavy lifting behind assembling physical topologies in your cluster.
The Python code for `heron` can be found in
[`heron/tools/cli`]({{% githubMaster %}}/heron/tools/cli).

Sample configurations for different Heron schedulers

* [Local scheduler](schedulers-local) config can be found in [`heron/config/src/yaml/conf/local`]({{% githubMaster %}}/heron/config/src/yaml/conf/local),
* [Aurora scheduler](schedulers-aurora-cluster) config can be found [`heron/config/src/yaml/conf/aurora`]({{% githubMaster %}}/heron/config/src/yaml/conf/aurora).

### Heron Tracker

The Python code for the [Heron Tracker](user-manuals-heron-tracker-runbook) can be
found in [`heron/tools/tracker`]({{% githubMaster %}}/heron/tools/tracker).

The Tracker is a web server written in Python. It relies on the
[Tornado](http://www.tornadoweb.org/en/stable/) framework. You can add new HTTP
routes to the Tracker in
[`main.py`]({{% githubMaster %}}/heron/tools/tracker/src/python/main.py) and
corresponding handlers in the
[`handlers`]({{% githubMaster %}}/heron/tools/tracker/src/python/handlers) directory.

### Heron UI

The Python code for the [Heron UI](user-manuals-heron-ui) can be found in
[`heron/tools/ui`]({{% githubMaster %}}/heron/tools/ui).

Like Heron Tracker, Heron UI is a web server written in Python that relies on
the [Tornado](http://www.tornadoweb.org/en/stable/) framework. You can add new
HTTP routes to Heron UI in
[`main.py`]({{% githubMaster %}}/heron/web/source/python/main.py) and corresponding
handlers in the [`handlers`]({{% githubMaster %}}/heron/web/source/python/handlers)
directory.

### Heron Shell

The Python code for the [Heron Shell](user-manuals-heron-shell) can be
found in [`heron/shell`]({{% githubMaster %}}/heron/shell). The HTTP handlers and
web server are defined in
[`main.py`]({{% githubMaster %}}/heron/shell/src/python/main.py) while the HTML,
JavaScript, CSS, and images for the web UI can be found in the
[`assets`]({{% githubMaster %}}/heron/shell/assets) directory.

## Tests

There are a wide variety of tests for Heron that are scattered throughout the
codebase. For more info see [Testing Heron](compiling-running-tests).
