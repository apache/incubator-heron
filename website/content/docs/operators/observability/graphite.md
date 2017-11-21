---
title: Heron observability with Graphite
---

To observe Heron's runtime metrics, you can integrate Heron and the Heron UI with
[Graphite](http://graphite.readthedocs.io/en/latest/overview.html) and
[Grafana](http://grafana.org/).

To accomplish this, you need to do the following:

* Export topology metrics from Heron
* Gather Aurora and Linux metrics with Diamond
* Set up a scripted dashboard with Grafana
* Configure the Heron UI to link to Grafana

### Exporting Topology Metrics From Heron

Heron supports custom metric exporters from the Metrics Manager. You can either build your own Graphite metrics sink or use the [provided Graphite sink](/docs/contributors/custom-metrics-sink/).

### Gathering Metrics From Aurora

In addition to the topology-specific data available from Heron, much more data is available directly
from Aurora and the Linux kernel. These can help identify many operational problems, such as
CPU throttling or crashes.

A common way to collect data from these sources is via a system metrics collection daemon such as
[collectd](https://collectd.org/) or [Diamond](https://github.com/python-diamond/Diamond)

Diamond has the following relevant collectors available:

* [Aurora](https://github.com/python-diamond/Diamond/tree/master/src/collectors/aurora)


### Creating A Scripted Grafana Dashboard

A convienent way to view topology-specific metrics in Graphite is to create a
[scripted dashboard in Grafana](http://docs.grafana.org/reference/scripting/). The scripted
dashboard should accept information such as the topology name as query arguments, which will allow
the Heron UI to deep link to a specific topology's dashboard.

Suggested targets for the scripted dashboard include:

**Component Backpressure**:

```python
'aliasByNode(sortByMaxima(highestAverage(heron.' + topology_name + '.stmgr.stmgr-*.
time_spent_back_pressure_by_compid.*, 5)), 5)'
```

**Fail Count by Component**:

```python
'sumSeriesWithWildcards(aliasByNode(heron.' + topology_name + '.*.*.fail-count.default,2),3)'`
```

**CPU Throttling periods**:

```python
aliasByNode(nonNegativeDerivative(mesos.tasks.prod.*.' + topology_name + '.*.cpu.
nr_throttled), 4,5)
```

**JVM Deaths**:

```python
'aliasByNode(drawAsInfinite(maximumAbove(removeAboveValue(heron.' + topology_name + '.*.*.jvm.
uptime-secs, 60),1)),2,3)'
```

**Top 5 worst GC components**:

```python
'aliasByNode(highestMax(nonNegativeDerivative(heron.' + topology_name + '.*.*.jvm.gc-time-ms.
PS-*),5), 2,3,6)'
```

### Configuring The Heron UI Link To Grafana

Finally, you can configure the Heron UI to deep link to scripted dashboards by specifying an
[observability URL format]
(https://github.com/twitter/heron/blob/master/heron/config/src/yaml/tracker/heron_tracker.yaml)
(`viz.url.format`) in the Heron Tracker's configuration. This will add topology-specific buttons to
the Heron UI enabling you to drill-down into your Grafana dashboards.
