# The Heron Codebase

This document contains information about the Heron codebase intended primarily
for developers who want to contribute to Heron.

If you're looking for documentation about developing topologies for a Heron
cluster, see [Building Topologies](../developers/topologies.html) instead.

## Languages

The primary programming languages for Heron are C++, Java, and Python.

* **C++** is used for most of Heron's core components, including the [Topology
Master](../concepts/architecture.html#topology-master), [Stream
Manager](../concepts/architecture.html#stream-manager), and [Metrics
Manager](../concepts/architecture.html#metrics-manager).

* **Java** is used primarily for Heron's [topology API](../concepts/topologies.html)
and is currently the only language in which topologies can be written.
Instructions can be found in [Building
Topologies](../developers/topologies.html), while API documentation for the Java
API can be found [here](../api/topology/index.html).

* **Python** is used primarily for Heron's [CLI
interface](../operators/heron-cli.html) and UI components such as [Heron
UI](../operators/heron-ui.html) and the [Heron
Tracker](../operators/heron-tracker.html).

## Main Tools

The table below lists the tools used extensively within Heron's codebase.

* **Build tool** &mdash; Heron uses [Bazel](http://bazel.io/) as its build tool.
Information on setting up and using Bazel for Heron can be found in [Compiling
Heron](../developers/compiling.html).

* **Inter-component communication** &mdash; [Protocol
Buffers](https://developers.google.com/protocol-buffers/?hl=en). Most `.proto`
definition files can be found in [`heron/proto`]({{ book.root_url
}}/heron/proto).

* **Cluster coordination** &mdash; Heron relies heavily on ZooKeeper for cluster
coordination, be it for [Mesos/Aurora](../operators/deployment/aurora.html),
[Mesos alone](../operators/deployment/mesos.html), or a [custom
scheduler](custom-scheduler.html) that you build.

## Components

### Topology API

Heron's API for writing topologies is written in Java. The code for this API can
be found in [`heron/api`]({{ book.root_url }}/heron/api).

Documentation for writing topologies can be found in [Building
Topologies](../developers/topologies.html), while API documentation can be found
[here](../api/topology/index.html).

### Metrics Manager

If you'd like to implement your own custom metrics handler (known as a **metrics
sink**), see [Implementing a Custom Metrics Sink](custom-metrics-sink.html).

### Heron Tracker

The Python code for the [Heron Tracker](../operators/heron-tracker.html) is in
[`heron/tracker`]({{ book.root_url }}/heron/tracker).

The Tracker is a web server written in Python. It relies on the
[Tornado](http://www.tornadoweb.org/en/stable/) framework. You can add new HTTP
routes to the Tracker in [application definition]({{ book.root_url
}}/heron/tracker/src/python/main.py) and corresponding handlers in the
[`/handlers`]({{ book.root_url }}/heron/tracker/src/python/handlers) directory.

### Heron UI

The code for the [Heron UI](../operators/heron-ui.html) is in [`heron/web`]({{
book.root_url }}/heron/web).

### 

## Example Topologies

Heron's codebase includes a wide variety of example
[topologies](../concepts/topologies.html) built using Heron's topology API for
Java. Those examples can be found in [`heron/examples`]({{ book.root_url
}}/heron/examples).
