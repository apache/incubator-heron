---
title: The Heron Codebase
---

This document contains information about the Heron codebase intended primarily
for developers who want to contribute to Heron.

If you're looking for documentation about developing topologies for a Heron
cluster, see [Building Topologies](../developers/topologies.html) instead.

## Languages

The primary programming languages for Heron are C++, Java, and Python.

* **C++ 11** is used for most of Heron's core components, including the
[Topology Master](../../concepts/architecture#topology-master), [Stream
Manager](../../concepts/architecture#stream-manager), and [Metrics
Manager](../../concepts/architecture#metrics-manager).

* **Java 8** is used primarily for Heron's [topology
API](../../concepts/topologies) and is currently the only language in which
topologies can be written. Instructions can be found in [Building
Topologies](../developers/topologies.html), while API documentation for the Java
API can be found [here](../api/topology/index.html). Please note that Heron
topologies do not require Java 8 and can be written in Java 7 or later.

* **Python 2** (specifically 2.7) is used primarily for Heron's [CLI
interface](../../operators/heron-cli) and UI components such as [Heron
UI](../operators/heron-ui.html) and the [Heron
Tracker](../operators/heron-tracker.html).

## Main Tools

* **Build tool** &mdash; Heron uses [Bazel](http://bazel.io/) as its build tool.
Information on setting up and using Bazel for Heron can be found in [Compiling
Heron](../developers/compiling.html).

* **Inter-component communication** &mdash; Heron uses [Protocol
Buffers](https://developers.google.com/protocol-buffers/?hl=en) for
communication between components. Most `.proto` definition files can be found in
[`heron/proto`]({{book.root_url}}/heron/proto).

* **Cluster coordination** &mdash; Heron relies heavily on ZooKeeper for cluster
coordination, be it for [Mesos/Aurora](../operators/deployment/aurora.html),
[Mesos alone](../operators/deployment/mesos.html), or for a [custom
scheduler](custom-scheduler.html) that you build. More information on ZooKeeper
components in the codebase can be found in the [State
Management](#state-management) section below.

## Common Utilities

The [`heron/common`]({{book.root_url}}/heron/common) contains a variety of
utilities for each of Heron's languages, including useful constants, file
utilities, networking interfaces, and more.

## Cluster Scheduling

Heron supports three cluster schedulers out of the box:
[Mesos](../operators/deployment/mesos.html),
[Aurora](../operators/deployment/aurora.html), and a [local
scheduler](../operators/deployment/local.html). The Java code for each of those
schedulers, as well as for the underlying scheduler API, can be found in
[`heron/scheduler`]({{book.root_url}}/heron/scheduler).

Info on custom schedulers can be found in [Implementing a Custom
Scheduler](custom-scheduler.html); info on the currently available schedulers
can be found in [Deploying Heron on
Aurora](../operators/deployment/aurora.html), [Deploying Heron on
Mesos](../operators/deployment/mesos.html), and [Local
Deployment](../operators/deployment/local.html).

## State Management

The parts of Heron's codebase related to
[ZooKeeper](http://zookeeper.apache.org/) are mostly contained in
[`heron/state`]({{book.root_url}}/heron/state). There are ZooKeeper-facing
interfaces for [C++]({{book.root_url}}/heron/state/src/cpp),
[Java]({{book.root_url}}/heron/state/src/java), and
[Python]({{book.root_url}}/heron/state/src/python) that are used in a variety of
Heron components.

## Topology Components

### Topology Master

The C++ code for Heron's [Topology
Master](../../concepts/architecture#topology-master) is written in C++ can be
found in [`heron/tmaster`]({{book.root_url}}/heron/tmaster).

### Stream Manager

The C++ code for Heron's [Stream
Manager](../../concepts/architecture#stream-manager) can be found in
[`heron/stmgr`]({{book.root_url}}/heron/stmgr).

### Heron Instance

The Java code for [Heron
instances](../../concepts/architecture#heron-instance) can be found in
[`heron/instance`]({{book.root_url}}/heron/instance).

### Metrics Manager

The Java code for Heron's [Metrics
Manager](../../concepts/architecture#metrics-manager) can be found in
[`heron/metricsmgr`]({{book.root_url}}/heron/metricsmgr).

If you'd like to implement your own custom metrics handler (known as a **metrics
sink**), see [Implementing a Custom Metrics Sink](custom-metrics-sink.html).

## Developer APIs

### Topology API

Heron's API for writing topologies is written in Java. The code for this API can
be found in [`heron/api`]({{book.root_url}}/heron/api).

Documentation for writing topologies can be found in [Building
Topologies](../developers/topologies.html), while API documentation can be found
[here](../api/topology/index.html).

### Local Mode

Heron enables you to run topologies in [local
mode](../developers/topologies.html#local-mode) for debugging purposes.

The Java API for local mode can be found in
[`heron/localmode`]({{book.root_url}}/heron/localmode).

### Example Topologies

Heron's codebase includes a wide variety of example
[topologies](../../concepts/topologies) built using Heron's topology API for
Java. Those examples can be found in
[`heron/examples`]({{book.root_url}}/heron/examples).

## User Interface Components

### Heron CLI

Heron has a tool called `heron-cli` that is used to both provide a CLI interface
for [managing topologies](../../operators/heron-cli) and to perform much of
the heavy lifting behind assembling physical topologies in your cluster.

The Python code for `heron-cli` can be found in
[`heron/cli2`]({{book.root_url}}/heron/cli2). The entire logic of `heron-cli` is
contained in [`cli.py`]({{book.root_url}}/heron/cli2/src/python/cli.py).

The default configuration for Heron schedulers is found in
[`scheduler.conf`]({{book.root_url}}/heron/cli2/src/python/scheduler.conf),
while configuration for Heron's [local
scheduler](../operators/deployment/local.html) can be found in
[`local_scheduler.conf`]({{book.root_url}}/heron/cli2/src/python/local_scheduler.conf).

### Heron UI

The Python code for the [Heron UI](../operators/heron-ui.html) can be found in
[`heron/web`]({{book.root_url}}/heron/web).

Like Heron Tracker, Heron UI is a web server written in Python that relies on
the [Tornado](http://www.tornadoweb.org/en/stable/) framework. You can add new
HTTP routes to Heron UI in
[`main.py`]({{book.root_url}}/heron/web/source/python/main.py) and corresponding
handlers in the [`handlers`]({{root.book_url}}/heron/web/source/python/handlers)
directory.

### Heron Tracker

The Python code for the [Heron Tracker](../operators/heron-tracker.html) can be
found in [`heron/tracker`]({{book.root_url}}/heron/tracker).

The Tracker is a web server written in Python. It relies on the
[Tornado](http://www.tornadoweb.org/en/stable/) framework. You can add new HTTP
routes to the Tracker in
[`main.py`]({{book.root_url}}/heron/tracker/src/python/main.py) and
corresponding handlers in the [`handlers`]({{ book.root_url
}}/heron/tracker/src/python/handlers) directory.

### Heron Shell

The Python code for the [Heron Shell](../operators/heron-shell.html) can be
found in [`heron/shell`]({{book.root_url}}/heron/shell). The HTTP handlers and
web server are defined in
[`main.py`]({{book.root_url}}/heron/shell/src/python/main.py) while the HTML,
JavaScript, CSS, and images for the web UI can be found in the
[`assets`]({{book.root_url}}/heron/shell/assets) directory.

## Tests

There are a wide variety of tests for Heron that are scattered throughout the
codebase. For more info see [Testing Heron](testing.html).
