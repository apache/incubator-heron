---
id: version-0.20.0-incubating-user-manuals-tracker-rest
title: Heron Tracker REST API
sidebar_label: Heron Tracker REST API
original_id: user-manuals-tracker-rest
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

### JSON Interface

All Heron Tracker endpoints return a JSON object with the following information:

* `status` --- One of the following: `success`, `failure`.
* `executiontime` --- The time taken to return the HTTP result, in seconds.
* `message` --- Some endpoints return special messages in this field for certain
  requests. Often, this field will be an empty string. A `failure` status will
  always have a message.
* `result` --- The result payload of the request. The contents will depend on
  the endpoint.
* `version` --- The Tracker API version.

### Endpoints

* `/` (redirects to `/topologies`)
* [`/clusters`](#clusters)
* [`/topologies`](#topologies)
* [`/topologies/states`](#topologies_states)
* [`/topologies/info`](#topologies_info)
* [`/topologies/logicalplan`](#topologies_logicalplan)
* [`/topologies/physicalplan`](#topologies_physicalplan)
* [`/topologies/executionstate`](#topologies_executionstate)
* [`/topologies/schedulerlocation`](#topologies_schedulerlocation)
* [`/topologies/metrics`](#topologies_metrics)
* [`/topologies/metricstimeline`](#topologies_metricstimeline)
* [`/topologies/metricsquery`](#topologies_metricsquery)
* [`/topologies/containerfiledata`](#topologies_containerfiledata)
* [`/topologies/containerfilestats`](#topologies_containerfilestats)
* [`/topologies/exceptions`](#topologies_exceptions)
* [`/topologies/exceptionsummary`](#topologies_exceptionsummary)
* [`/topologies/pid`](#topologies_pid)
* [`/topologies/jstack`](#topologies_jstack)
* [`/topologies/jmap`](#topologies_jmap)
* [`/topologies/histo`](#topologies_histo)
* [`/machines`](#machines)

All of these endpoints are documented in the sections below.

---

### <a name="clusters">/clusters</a>

Returns JSON list of all the clusters.

---

### <a name="topologies">/topologies</a>

Returns JSON describing all currently available topologies

```bash
$ curl "http://heron-tracker-url/topologies?cluster=cluster1&environ=devel"
```

#### Parameters

* `cluster` (optional) --- The cluster parameter can be used to filter
   topologies that are running in this cluster.
* `environ` (optional) --- The environment parameter can be used to filter
   topologies that are running in this environment.

---

### <a name="topologies_logicalplan">/topologies/logicalplan</a>

Returns a JSON representation of the [logical plan](heron-topology-concepts#logical-plan) of a topology.

```bash
$ curl "http://heron-tracker-url/topologies/logicalplan?cluster=cluster1&environ=devel&topology=topologyName"
```

#### Parameters

* `cluster` (required) --- The cluster in which the topology is running
* `environ` (required) --- The environment in which the topology is running
* `topology` (required) --- The name of the topology

The resulting JSON contains the following

* `spouts` --- A set of JSON objects representing each spout in the topology.
  The following information is listed for each spout:
  * `source` --- The source of tuples for the spout.
  * `type` --- The type of the spout, e.g. `kafka`, `kestrel`, etc.
  * `outputs` --- A list of streams to which the spout outputs tuples.
* `bolts` --- A set of JSON objects representing each bolt in the topology.
  * `outputs` --- A list of streams to which the bolt outputs tuples.
  * `inputs` --- A list of inputs for the bolt. An input is represented by
  JSON dictionary containing following information.
      * `component_name` --- Name of the component this bolt is receiving tuples from.
      * `stream_name` --- Name of the stream from which the tuples are received.
      * `grouping` --- Type of grouping used to receive tuples, example `SHUFFLE` or `FIELDS`.

---

### <a name="topologies_physicalplan">/topologies/physicalplan</a>

Returns a JSON representation of the [physical plan](heron-topology-concepts#physical-plan) of a topology.

```bash
$ curl "http://heron-tracker-url/topologies/physicalplan?cluster=datacenter1&environ=prod&topology=topologyName"
```

#### Parameters

* `cluster` (required) --- The cluster in which the topology is running
* `environ` (required) --- The environment in which the topology is running
* `topology` (required) --- The name of the topology

The resulting JSON contains following information

* All spout and bolt components, with lists of their instances.
* `stmgrs` --- A list of JSON dictionary, containing following information of each stream manager.
  * `host` --- Hostname of the machine this container is running on.
  * `pid` --- Process ID of the stream manager.
  * `cwd` --- Absolute path to the directory from where container was launched.
  * `joburl` --- URL to browse the `cwd` through `heron-shell`.
  * `shell_port` --- Port to access `heron-shell`.
  * `logfiles` --- URL to browse instance log files through `heron-shell`.
  * `id` --- ID for this stream manager.
  * `port` --- Port at which this stream manager accepts connections from other stream managers.
  * `instance_ids` --- List of instance IDs that constitute this container.
* `instances` --- A list of JSON dictionaries containing following information for each instance
  * `id` --- Instance ID.
  * `name` --- Component name of this instance.
  * `logfile` --- Link to log file for this instance, that can be read through `heron-shell`.
  * `stmgrId` --- Its stream manager's ID.
* `config` --- Various topology configs. Some of the examples are:
  * `topology.message.timeout.secs` --- Time after which a tuple should be considered as failed.
  * `topology.acking` --- Whether acking is enabled or not.

---

### <a name="topologies_schedulerlocation">/topologies/schedulerlocation</a>

Returns a JSON representation of the scheduler location of the topology.

```bash
$ curl "http://heron-tracker-url/topologies/schedulerlocation?cluster=datacenter1&environ=prod&topology=topologyName"
```

#### Parameters

* `cluster` (required) --- The cluster in which the topology is running
* `environ` (required) --- The environment in which the topology is running
* `topology` (required) --- The name of the topology

The `SchedulerLocation` mainly contains the link to the job on the scheduler,
for example, the Aurora page for the job.

---

### <a name="topologies_executionstate">/topologies/executionstate</a>

Returns a JSON representation of the execution state of the topology.

```bash
$ curl "http://heron-tracker-url/topologies/executionstate?cluster=datacenter1&environ=prod&topology=topologyName"
```

#### Parameters

* `cluster` (required) --- The cluster in which the topology is running
* `environ` (required) --- The environment in which the topology is running
* `topology` (required) --- The name of the topology

Each execution state object lists the following:

* `cluster` --- The cluster in which the topology is running
* `environ` --- The environment in which the topology is running
* `role` --- The role with which the topology was launched
* `jobname` --- Same as topology name
* `submission_time` --- The time at which the topology was submitted
* `submission_user` --- The user that submitted the topology (can be same as `role`)
* `release_username` --- The user that generated the Heron release for the
  topology
* `release_version` --- Release version
* `has_physical_plan` --- Whether the topology has a physical plan
* `has_tmaster_location` --- Whether the topology has a Topology Master Location
* `has_scheduler_location` --- Whether the topology has a Scheduler Location
* `viz` --- Metric visualization UI URL for the topology if it was [configured](user-manuals-heron-tracker-runbook)

---

### <a name="topologies_states">/topologies/states</a>

Returns a JSON list of execution states of topologies in all the cluster.

```bash
$ curl "http://heron-tracker-url/topologies/states?cluster=cluster1&environ=devel"
```

#### Parameters

* `cluster` (optional) --- The cluster parameter can be used to filter
   topologies that are running in this cluster.
* `environ` (optional) --- The environment parameter can be used to filter
   topologies that are running in this environment.

---

### <a name="topologies_info">/topologies/info</a>

Returns a JSON representation of a dictionary containing logical plan, physical plan,
execution state, scheduler location and TMaster location for a topology, as described above.
`TMasterLocation` is the location of the TMaster, including its host,
port, and the heron-shell port that it exposes.

#### Parameters

* `cluster` (required) --- The cluster in which the topology is running
* `environ` (required) --- The environment in which the topology is running
* `topology` (required) --- The name of the topology

---

### <a name="topologies_containerfilestats">/topologies/containerfilestats</a>

Returns the file stats for a container. This is the output of the command `ls -lh` when run
in the directory where the heron-controller launched all the processes.

This endpoint is mainly used by ui for exploring files in a container.

#### Parameters

* `cluster` (required) --- The cluster in which the topology is running
* `environ` (required) --- The environment in which the topology is running
* `topology` (required) --- The name of the topology
* `container` (required) --- Container ID
* `path` (optional) --- Path relative to the directory where heron-controller is launched.
   Paths are not allowed to start with a `/` or contain a `..`.

---

### <a name="topologies_containerfiledata">/topologies/containerfiledata</a>

Returns the file data for a file of a container.

This endpoint is mainly used by ui for exploring files in a container.

#### Parameters

* `cluster` (required) --- The cluster in which the topology is running
* `environ` (required) --- The environment in which the topology is running
* `topology` (required) --- The name of the topology
* `container` (required) --- Container ID
* `path` (required) --- Path to the file relative to the directory where heron-controller is launched.
   Paths are not allowed to start with a `/` or contain a `..`.
* `offset` (required) --- Offset from the beggining of the file.
* `length` (required) --- Number of bytes to be returned.

---

### <a name="topologies_metrics">/topologies/metrics</a>

Returns a JSON map of instances of the topology to their respective metrics.
To filter instances returned use the `instance` parameter discussed below.


Note that these metrics come from TMaster, which only holds metrics
for last 3 hours minutely data, as well as cumulative values. If the `interval`
is greater than `10800` seconds, the values will be for all-time metrics.

#### Parameters

* `cluster` (required) --- The cluster in which the topology is running
* `environ` (required) --- The environment in which the topology is running
* `topology` (required) --- The name of the topology
* `component` (required) --- Component name
* `metricname` (required, repeated) --- Names of metrics to fetch
* `interval` (optional) --- For how many seconds, the metrics should be fetched for (max 10800 seconds)
* `instance` (optional) --- IDs of the instances. If not present, return for all the instances.

---

### <a name="topologies_metricstimeline">/topologies/metricstimeline</a>

Returns a JSON map of instances of the topology to their respective metrics timeline.
To filter instances returned use the `instance` parameter discussed below.

The difference between this and `/metrics` endpoint above, is that `/metrics` will report
cumulative value over the period of `interval` provided. On the other hand, `/metricstimeline`
endpoint will report minutely values for each metricname for each instance.

Note that these metrics come from TMaster, which only holds metrics
for last 3 hours minutely data, as well as cumulative all-time values. If the starttime
is older than 3 hours ago, those minutes would not be part of the response.

#### Parameters

* `cluster` (required) --- The cluster in which the topology is running
* `environ` (required) --- The environment in which the topology is running
* `topology` (required) --- The name of the topology
* `component` (required) --- Component name
* `metricname` (required, repeated) --- Names of metrics to fetch
* `starttime` (required) --- Start time for the metrics (must be within last 3 hours)
* `endtime` (required) --- End time for the metrics (must be within last 3 hours,
   and greater than `starttime`)
* `instance` (optional) --- IDs of the instances. If not present, return for all the instances.

### <a name="topologies_metricsquery">/topologies/metricsquery</a>

Executes the metrics query for the topology and returns the result in form of minutely timeseries.
A detailed description of query language is given [below](#metricsquery).

Note that these metrics come from TMaster, which only holds metrics
for last 3 hours minutely data, as well as cumulative all-time values. If the starttime
is older than 3 hours ago, those minutes would not be part of the response.

#### Parameters

* `cluster` (required) --- The cluster in which the topology is running
* `environ` (required) --- The environment in which the topology is running
* `topology` (required) --- The name of the topology
* `starttime` (required) --- Start time for the metrics (must be within last 3 hours)
* `endtime` (required) --- End time for the metrics (must be within last 3 hours,
   and greater than `starttime`)
* `query` (required) --- Query to be executed

---

### <a name="topologies_exceptionsummary">/topologies/exceptionsummary</a>

Returns summary of the exceptions for the component of the topology.
Duplicated exceptions are combined together and includes the number of
occurances, first occurance time and latest occurance time.

#### Parameters

* `cluster` (required) --- The cluster in which the topology is running
* `environ` (required) --- The environment in which the topology is running
* `topology` (required) --- The name of the topology
* `component` (required) --- Component name
* `instance` (optional) --- IDs of the instances. If not present, return for all the instances.

---

### <a name="topologies_exceptions">/topologies/exceptions</a>

Returns all exceptions for the component of the topology.

#### Parameters

* `cluster` (required) --- The cluster in which the topology is running
* `environ` (required) --- The environment in which the topology is running
* `topology` (required) --- The name of the topology
* `component` (required) --- Component name
* `instance` (optional) --- IDs of the instances. If not present, return for all the instances.

---

### <a name="topologies_pid">/topologies/pid</a>

Returns the PID of the instance JVM process.

#### Parameters

* `cluster` (required) --- The cluster in which the topology is running
* `environ` (required) --- The environment in which the topology is running
* `topology` (required) --- The name of the topology
* `instance` (required) --- Instance ID

---

### <a name="topologies_jstack">/topologies/jstack</a>

Returns the thread dump of the instance JVM process.

#### Parameters

* `cluster` (required) --- The cluster in which the topology is running
* `environ` (required) --- The environment in which the topology is running
* `topology` (required) --- The name of the topology
* `instance` (required) --- Instance ID

---

### <a name="topologies_jmap">/topologies/jmap</a>

Issues the `jmap` command for the instance, and saves the result in a file.
Returns the path to the file that can be downloaded externally.

#### Parameters

* `cluster` (required) --- The cluster in which the topology is running
* `environ` (required) --- The environment in which the topology is running
* `topology` (required) --- The name of the topology
* `instance` (required) --- Instance ID

---

### <a name="topologies_histo">/topologies/histo</a>

Returns histogram for the instance JVM process.

#### Parameters

* `cluster` (required) --- The cluster in which the topology is running
* `environ` (required) --- The environment in which the topology is running
* `topology` (required) --- The name of the topology
* `instance` (required) --- Instance ID

---

### <a name="machines">/machines</a>

Returns JSON describing all machines that topologies are running on.

```bash
$ curl "http://heron-tracker-url/machines?topology=mytopology1&cluster=cluster1&environ=prod"
```

#### Parameters

* `cluster` (optional) --- The cluster parameter can be used to filter
   machines that are running the topologies in this cluster only.
* `environ` (optional) --- The environment parameter can be used to filter
   machines that are running the topologies in this environment only.
* `topology` (optional, repeated) --- Name of the topology. Both `cluster`
   and `environ` are required if the `topology` parameter is present

---

### <a name="metricsquery">Metrics Query Language</a>

Metrics queries are useful when some kind of aggregated values are required. For example,
to find the total number of tuples emitted by a spout, `SUM` operator can be used, instead
of fetching metrics for all the instances of the corresponding component, and then summing them.

#### Terminology

1. Univariate Timeseries --- A timeseries is called univariate if there is only one set
of minutely data. For example, a timeseries representing the sums of a number of timeseries
would be a univariate timeseries.
2. Multivariate Timeseries --- A set of multiple timeseries is collectively called multivariate.
Note that these timeseries are associated with their instances.

#### Operators

##### TS

```text
TS(componentName, instance, metricName)
```

Example:

```text
TS(component1, *, __emit-count/stream1)
```

Time Series Operator. This is the basic operator that is responsible for getting metrics from TMaster.
Accepts a list of 3 elements:

1. componentName
2. instance - can be "*" for all instances, or a single instance ID
3. metricName - Full metric name with stream id if applicable

Returns a univariate time series in case of a single instance id given, otherwise returns
a multivariate time series.

---

##### DEFAULT

```text
DEFAULT(0, TS(component1, *, __emit-count/stream1))
```
If the second operator returns more than one timeline, so will the
DEFAULT operator.

```text
DEFAULT(100.0, SUM(TS(component2, *, __emit-count/default))) <--
```
Second operator can be any operator

Default Operator. This operator is responsible for filling missing values in the metrics timeline.
Must have 2 arguments

1. First argument is a numeric constant representing the number to fill the missing values with
2. Second one must be one of the operators, that return the metrics timeline

Returns a univariate or multivariate time series, based on what the second operator is.

---

##### SUM

```text
SUM(TS(component1, instance1, metric1), DEFAULT(0, TS(component1, *, metric2)))
```

Sum Operator. This operator is used to take sum of all argument time series. It can have any number of arguments,
each of which must be one of the following two types:

1. Numeric constants, which will fill in the missing values as well, or
2. Operator, which returns one or more timelines

Returns only a single timeline representing the sum of all time series for each timestamp.
Note that "instance" attribute is not there in the result.

---

##### MAX

```text
MAX(100, TS(component1, *, metric1))
```

Max Operator. This operator is used to find max of all argument operators for each individual timestamp.
Each argument must be one of the following types:

1. Numeric constants, which will fill in the missing values as well, or
2. Operator, which returns one or more timelines

Returns only a single timeline representing the max of all the time series for each timestamp.
Note that "instance" attribute is not included in the result.

---

##### PERCENTILE

```text
PERCENTILE(99, TS(component1, *, metric1))
```

Percentile Operator. This operator is used to find a quantile of all timelines retuned by the arguments, for each timestamp.
This is a more general type of query similar to MAX. Note that `PERCENTILE(100, TS...)` is equivalent to `Max(TS...)`.
Each argument must be either constant or Operators.
First argument must always be the required Quantile.

1. Quantile (first argument) - Required quantile. 100 percentile = max, 0 percentile = min.
2. Numeric constants will fill in the missing values as well,
3. Operator - which returns one or more timelines

Returns only a single timeline representing the quantile of all the time series
for each timestamp. Note that "instance" attribute is not there in the result.

---

##### DIVIDE

```text
DIVIDE(TS(component1, *, metrics1), 100)
```

Divide Operator. Accepts two arguments, both can be univariate or multivariate.
Each can be of one of the following types:

1. Numeric constant will be considered as a constant time series for all applicable timestamps, they will not fill the missing values
2. Operator - returns one or more timelines

Three main cases are:

1. When both operands are multivariate
    1. Divide operation will be done on matching data, that is, with same instance id.
    2. If the instances in both the operands do not match, error is thrown.
    3. Returns multivariate time series, each representing the result of division on the two corresponding time series.
2. When one operand is univariate, and other is multivariate
    1. This includes division by constants as well.
    2. The univariate operand will participate with all time series in multivariate.
    3. The instance information of the multivariate time series will be preserved in the result.
    4. Returns multivariate time series.
3. When both operands are univariate
    1. Instance information is ignored in this case
    2. Returns univariate time series which is the result of division operation.

---

##### MULTIPLY

```text
MULTIPLY(10, TS(component1, *, metrics1))
```

Multiply Operator. Has same conditions as division operator. This is to keep the API simple.
Accepts two arguments, both can be univariate or multivariate. Each can be of one of the following types:

1. Numeric constant will be considered as a constant time series for all applicable timestamps, they will not fill the missing values
2. Operator - returns one or more timelines

Three main cases are:

1. When both operands are multivariate
    1. Multiply operation will be done on matching data, that is, with same instance id.
    2. If the instances in both the operands do not match, error is thrown.
    3. Returns multivariate time series, each representing the result of multiplication
        on the two corresponding time series.
2. When one operand is univariate, and other is multivariate
    1. This includes multiplication by constants as well.
    2. The univariate operand will participate with all time series in multivariate.
    3. The instance information of the multivariate time series will be preserved in the result.
    4. Returns multivariate timeseries.
3. When both operands are univariate
    1. Instance information is ignored in this case
    2. Returns univariate timeseries which is the result of multiplication operation.

---

##### SUBTRACT

```text
SUBTRACT(TS(component1, instance1, metrics1), TS(componet1, instance1, metrics2))

SUBTRACT(TS(component1, instance1, metrics1), 100)
```

Subtract Operator. Has same conditions as division operator. This is to keep the API simple.
Accepts two arguments, both can be univariate or multivariate. Each can be of one of the following types:

1. Numeric constant will be considered as a constant time series for all applicable timestamps, they will not fill the missing values
2. Operator - returns one or more timelines

Three main cases are:

1. When both operands are multivariate
    1. Subtract operation will be done on matching data, that is, with same instance id.
    2. If the instances in both the operands do not match, error is thrown.
    3. Returns multivariate time series, each representing the result of subtraction
        on the two corresponding time series.
2. When one operand is univariate, and other is multivariate
    1. This includes subtraction by constants as well.
    2. The univariate operand will participate with all time series in multivariate.
    3. The instance information of the multivariate time series will be preserved in the result.
    4. Returns multivariate time series.
3. When both operands are univariate
    1. Instance information is ignored in this case
    2. Returns univariate time series which is the result of subtraction operation.

---

##### RATE

```text
RATE(SUM(TS(component1, *, metrics1)))

RATE(TS(component1, *, metrics2))
```

Rate Operator. This operator is used to find rate of change for all timeseries.
Accepts a only a single argument, which must be an Operators which returns univariate or multivariate time series.
Returns univariate or multivariate time series based on the argument, with each timestamp value
corresponding to the rate of change for that timestamp.
