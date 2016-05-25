---
title: Heron UI
---

**Heron UI** is a user interface that uses the [Heron
Tracker](../../concepts/architecture#heron-tracker) to display detailed,
colorful visual representations of topologies, including the
[logical](../../concepts/topologies/#logical-plan) and [physical
plan](../../concepts/topologies#physical-plan) for each topology.

## Deploying Heron UI

Heron UI can run either on a machine managed by your Heron cluster's
[scheduler](../deployment) or outside of your cluster. You can
[compile](../../developers/compiling/compiling) a `heron-ui` executable to start up Heron UI:

```bash
$ cd /path/to/heron/binaries
$ ./heron-ui
```

By default, Heron UI runs on port 8889 and assumes that Heron Tracker is running
on `localhost`. You can change the port using the `--port` flag and the Heron
Tracker URL using the `--tracker_url` flag. Here's an example:

```bash
$ ./heron-ui --port=1234 --tracker_url=http://tracker-url
```

## UI Elements

In general, Heron UI is very intuitive and best learnt in an active, exploratory
manner. The sections below, however, provide a very basic guide in case any
aspects are unclear.

### Main Page

The main page of the UI, which displays a list of all topologies in the cluster,
looks like this:

![Heron UI](/img/ui.png)

You can use the upper nav bar to group topologies on the basis of both data
center/cluster and environment.

**Note**: The image above is from Twitter's internal Heron UI page and may not
be representative of the UI for your cluster.

### Topology Interface

When you click on the link for a specific topology on the main page, you'll be
taken to an interface that provides insight into the logical plan of the
topology and more (detailed in the sections below).

#### Logical Plan

The UI for each topology includes an image of the [logical
plan](../../concepts/topologies#logical-plan) for the spouts and bolts of the
topology. You can click on each component in the plan to see [metrics](#metrics)
for that component as well as a
[containers/instances](#containers-and-instances) map for that component.

![Logical Plan](/img/logical-plan.png)

#### Containers and Instances

When you first click on a topology's UI, the containers and instances map shows
you how many containers and instances are in the topology and provides a visual
representation like this:

![Containers](/img/containers.png)

To see how a specific spout or bolt is distributed throughout containers and
instances, click on that spout or bolt.

#### Topology Metrics

This component displays a variety of metrics from the last 3 minutes, 10
minutes, hour, and 3 hours. For a description of each metric, click the small
**?** icon next to the metric name. You can also toggle into and out of
colorblind-friendly mode.

![UI Metrics](/img/topology-metrics.png)

#### Component Metrics

![Component Metrics](/img/component-metrics.png)

#### Topology Info

![Topology Info](/img/topology-info.png)

This component contains basic information about the topology, including the
name, data center, role, and environment of the topology, as well as submission
date/time, and user who submitted the topology. On the far right, you'll see
links to a page displaying the topology's configuration as well as
topology-specific links for any other monitoring services.

#### Topology Counters

![Topology Counters](/img/topology-counters.png)
