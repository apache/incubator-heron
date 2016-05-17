---
title: Heron Code Organization
---

This document contains information about the Heron codebase intended primarily
for developers who want to contribute to Heron. The Heron codebase lives on
[github]({{% githubMaster %}}).

If you're looking for documentation about developing topologies for a Heron
cluster, see [Building Topologies](../developers/topologies.html) instead.

## Languages

The primary programming languages for Heron are C++, Java, and Python.

* **C++ 11** is used for most of Heron's core components, including the
[Topology Master](../../concepts/architecture#topology-master), and 
[Stream Manager](../../concepts/architecture#stream-manager).

* **Java 8** is used primarily for Heron's [topology
API](../../concepts/topologies), and [Heron Instance](../../concepts/architecture#heron-instance).
It is currently the only language in which topologies can be written. Instructions can be found 
in [Building Topologies](../developers/topologies.html), while API documentation for the Java
API can be found [here](/api/topology/index.html). Please note that Heron topologies do not 
require Java 8 and can be written in Java 7 or later.

* **Python 2** (specifically 2.7) is used primarily for Heron's [CLI
interface](../..//operators/heron-cli) and UI components such as [Heron
UI](../../operators/heron-ui) and the [Heron
Tracker](../../operators/heron-tracker).

## Main Tools

* **Build tool** &mdash; Heron uses [Bazel](http://bazel.io/) as its build tool.
Information on setting up and using Bazel for Heron can be found in [Compiling
Heron](../../developers/compiling/compiling).

* **Inter-component communication** &mdash; Heron uses [Protocol
Buffers](https://developers.google.com/protocol-buffers/?hl=en) for
communication between components. Most `.proto` definition files can be found in
[`heron/proto`]({{% githubMaster %}}/heron/proto).

* **Cluster coordination** &mdash; Heron relies heavily on ZooKeeper for cluster
coordination for distributed deployment, be it for [Mesos/Aurora](../../operators/deployment/schedulers/aurora),
[Mesos alone](../../operators/deployment/schedulers/mesos), or for a [custom
scheduler](../custom-scheduler) that you build. More information on ZooKeeper
components in the codebase can be found in the [State
Management](#state-management) section below.

## Common Utilities

The [`heron/common`]({{% githubMaster %}}/heron/common) contains a variety of
utilities for each of Heron's languages, including useful constants, file
utilities, networking interfaces, and more.

## Cluster Scheduling

Heron supports three cluster schedulers out of the box:
[Mesos](../../operators/deployment/schedulers/mesos),
[Aurora](../../operators/deployment/schedulers/aurora), and a [local
scheduler](../../operators/deployment/schedulers/local). The Java code for each of those
schedulers, as well as for the underlying scheduler API, can be found in
[`heron/schedulers`]({{% githubMaster %}}/heron/schedulers).

Info on custom schedulers can be found in [Implementing a Custom
Scheduler](../custom-scheduler); info on the currently available schedulers
can be found in [Deploying Heron on
Aurora](../../operators/deployment/schedulers/aurora), [Deploying Heron on
Mesos](../../operators/deployment/schedulers/mesos), and [Local
Deployment](../../operators/deployment/schedulers/local).

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
Master](../../concepts/architecture#topology-master) is written in C++ can be
found in [`heron/tmaster`]({{% githubMaster %}}/heron/tmaster).

### Stream Manager

The C++ code for Heron's [Stream
Manager](../../concepts/architecture#stream-manager) can be found in
[`heron/stmgr`]({{% githubMaster %}}/heron/stmgr).

### Heron Instance

The Java code for [Heron
instances](../../concepts/architecture#heron-instance) can be found in
[`heron/instance`]({{% githubMaster %}}/heron/instance).

### Metrics Manager

The Java code for Heron's [Metrics
Manager](../../concepts/architecture#metrics-manager) can be found in
[`heron/metricsmgr`]({{% githubMaster %}}/heron/metricsmgr).

If you'd like to implement your own custom metrics handler (known as a **metrics
sink**), see [Implementing a Custom Metrics Sink](../custom-metrics-sink).

## Developer APIs

### Topology API

Heron's API for writing topologies is written in Java. The code for this API can
be found in [`heron/api`]({{% githubMaster %}}/heron/api).

Documentation for writing topologies can be found in [Building
Topologies](../developers/topologies.html), while API documentation can be found
[here](/api/topology/index.html).

### Local Mode

Heron enables you to run topologies in [local
mode](../developers/topologies.html#local-mode) for debugging purposes.

The Java API for local mode can be found in
[`heron/localmode`]({{% githubMaster %}}/heron/localmode).

### Example Topologies

Heron's codebase includes a wide variety of example
[topologies](../../concepts/topologies) built using Heron's topology API for
Java. Those examples can be found in
[`heron/examples`]({{% githubMaster %}}/heron/examples).

## User Interface Components

### Heron CLI

Heron has a tool called `heron` that is used to both provide a CLI interface
for [managing topologies](../../operators/heron-cli) and to perform much of
the heavy lifting behind assembling physical topologies in your cluster.
The Python code for `heron` can be found in
[`heron/cli`]({{% githubMaster %}}/heron/cli). 

Sample configurations for different Heron schedulers 

* [Local scheduler](../../operators/deployment/schedulers/local) config can be found in [`heron/config/src/yaml/conf/local`]({{% githubMaster %}}/heron/config/src/yaml/conf/local),
* [Aurora scheduler](../../operators/deployment/schedulers/aurora) config can be found [`heron/config/src/yaml/conf/aurora`]({{% githubMaster %}}/heron/config/src/yaml/conf/aurora).

### Heron Tracker

The Python code for the [Heron Tracker](../../operators/heron-tracker) can be
found in [`heron/tracker`]({{% githubMaster %}}/heron/tracker).

The Tracker is a web server written in Python. It relies on the
[Tornado](http://www.tornadoweb.org/en/stable/) framework. You can add new HTTP
routes to the Tracker in
[`main.py`]({{% githubMaster %}}/heron/tracker/src/python/main.py) and
corresponding handlers in the
[`handlers`]({{% githubMaster %}}/heron/tracker/src/python/handlers) directory.

### Heron UI

The Python code for the [Heron UI](../../operators/heron-ui) can be found in
[`heron/ui`]({{% githubMaster %}}/heron/ui).

Like Heron Tracker, Heron UI is a web server written in Python that relies on
the [Tornado](http://www.tornadoweb.org/en/stable/) framework. You can add new
HTTP routes to Heron UI in
[`main.py`]({{% githubMaster %}}/heron/web/source/python/main.py) and corresponding
handlers in the [`handlers`]({{% githubMaster %}}/heron/web/source/python/handlers)
directory.

### Heron Shell

The Python code for the [Heron Shell](../../operators/heron-shell) can be
found in [`heron/shell`]({{% githubMaster %}}/heron/shell). The HTTP handlers and
web server are defined in
[`main.py`]({{% githubMaster %}}/heron/shell/src/python/main.py) while the HTML,
JavaScript, CSS, and images for the web UI can be found in the
[`assets`]({{% githubMaster %}}/heron/shell/assets) directory.

## Tests

There are a wide variety of tests for Heron that are scattered throughout the
codebase. For more info see [Testing Heron](../testing).
