## Heron Tracker REST API

### JSON Interface

All Heron Tracker endpoints return a JSON object with the following information:

* `status` -- One of the following: `success`, `failure`.
* `executiontime` -- The time it took to return the HTTP result, in seconds.
* `message` -- Some endpoints return special messages in this field for certain
  requests. Often, this field will be an empty string. A `fualure` status will
  always have a message.
* `result` -- The result payload of the request. The contents will depend on
  the endpoint.
* `version` -- The Tracker API version.

### Endpoints

* `/` (redirects to `/topologies`)
* `/clusters`
* `/topologies`
* `/topologies/states`
* `/topologies/info`
* `/topologies/logicalplan`
* `/topologies/physicalplan`
* `/topologies/executionstate`
* `/topologies/schedulerlocation`
* `/topologies/metrics`
* `/topologies/metricstimeline`
* `/topologies/metricsquery`
* `/topologies/containerfiledata`
* `/topologies/containerfilestats`
* `/topologies/exceptions`
* `/topologies/exceptionsummary`
* `/topologies/pid`
* `/topologies/jstack`
* `/topologies/jmap`
* `/topologies/histo`
* `/machines`

All of these endpoints are documented in the sections below.

***

### /cluster

Returns JSON list of all the clusters.

***

### /topologies

Returns JSON describing all currently available topologies

```bash
$ curl "http://heron-tracker-url/topologies?cluster=cluster1&environ=devel"
```

#### Parameters

* `cluster` (optional) --- The cluster.
* `environ` (optional) --- The environment.
  Example:

***

### /topologies/logicalplan

Returns a JSON object for the [logical plan](../../concepts/topologies#logical-plan) of a topology.

```bash
$ curl "http://heron-tracker-url/topologies/logicalplan?cluster=cluster1&environ=devel&topology=topologyName"
```

#### Parameters

* `cluster` (required) --- The cluster in which the topology is running
* `environ` (required) --- The environment in which the topology is running
* `topology` (required) --- The name of the topology

The resulting JSON contains following

* `spouts` --- A set of JSON objects representing each spout in the topology.
  The following information is listed for each spout:
  * `source` --- The source of tuples for the spout.
  * `type` --- The type of the spout, e.g. `kafka`, `kestrel`, etc.
  * `outputs` --- A list of streams to which the spout outputs tuples.
* `bolts` --- A set of JSON objects representing each bolt in the topology.
  * `outputs` --- A list of outputs for the bolt.
  * `inputs` --- A list of inputs for the bolt, containing input component and stream names.

***

### /topologies/physicalplan

Returns a JSON object for the [physical plan](../../concepts/topologies#physical-plan) of a topology.

```bash
$ curl "http://heron-tracker-url/topologies/physicalplan?cluster=datacenter1&environ=prod&topology=topologyName"
```

#### Required Parameters

* `cluster` --- The cluster in which the topology is running
* `environ` --- The environment
* `topology` --- The name of the topology

The resulting JSON contains following information

* Each spout and bolt components, with lists of their instances.
* `stmgrs` --- A list of JSON dictionary, containing host information of each stream manager.
* `instances` --- A list of JSON dictionary containing forllowing information for each instance
  * Link to logs for this instance
  * Link to job page for its container
  * Its stream manager's ID

****

### /topologies/schedulerlocation

The Scheduler location for the topology

```bash
$ curl "http://heron-tracker-url/topologies/schedulerlocation?cluster=datacenter1&environ=prod&topology=topologyName"
```

#### Required Parameters

* `cluster` --- The cluster in which the topology is running
* `environ` --- The environment in which the topology is running
* `topology` --- The name of the topology

The SchedulerLocation mainly contains the link of the job that is exposed by schedulers. Example,
a page that may be exposed by Aurora scheduler.

****

### /topologies/executionstate

The current execution state of a given topology.

```bash
$ curl "http://heron-tracker-url/topologies/executionstate?cluster=datacenter1&environ=prod&topology=topologyName"
```

#### Required Parameters

* `cluster` --- The cluster in which the topology is running
* `environ` --- The environment in which the topology is running
* `topology` --- The name of the topology

Each execution state object lists the following:

* `cluster` --- The cluster in which the topology is running
* `environ` --- The environment in which the topology is running
* `role` --- The role with which the topology was launched
* `jobname` --- Same as topology name
* `submission_time` --- The time at which the topology was submitted
* `submission_user` --- The user that submitted the topology (can be same as `role`)
* `release_username` --- The user that generated the Heron release for the
  topology
* `release_tag` --- Release version
* `release_version` --- Release version
* `has_physical_plan` --- Whether the physical plan for the topology is generated
* `has_tmaster_location` --- Whether the topology's Topology Master
  currently has a location
* `has_scheduler_location` --- Whether the topology has a Scheduler Location
  (timestamp in milliseconds)
* `viz` --- Viz URL for the topology if it was configured

***

### /topologies/states

Returns the JSON describing execution states of topologies in all the cluster.

```bash
$ curl "http://heron-tracker-url/topologies/states?cluster=cluster1&environ=devel"
```

#### Parameters

* `cluster` (optional) --- The cluster.
* `environ` (optional) --- The environment.

***

### /topologies/info

Returns logical plan, physical plan, execution state, scheduler location and tmaster location
for a topology, as these are described above. Tmaster location is the location of the topology
master, especially its host, port, and the heron-shell port that it exposes.

#### Parameters

* `cluster` (required) --- The cluster in which the topology is running
* `environ` (required) --- The environment in which the topology is running
* `topology` (required) --- The name of the topology

***

### /topologies/containerfilestats

Returns the file stats for a container. This is the output of the command `ls -lh` when run
in the directory where the heron-controller launched all the processes.

This endpoint is mainly used by ui for exploring files in a container.

#### Parameters

* `cluster` (required) --- The cluster in which the topology is running
* `environ` (required) --- The environment in which the topology is running
* `topology` (required) --- The name of the topology
* `container` (required) --- Container number
* `path` (optional) --- Path relative to the directory where heron-controller is launched.
   Paths are not allowed to start with a `/` or contain a `..`.

***

### /topologies/containerfiledata

Returns the file data for a file of a container.

This endpoint is mainly used by ui for exploring files in a container.

#### Parameters

* `cluster` (required) --- The cluster in which the topology is running
* `environ` (required) --- The environment in which the topology is running
* `topology` (required) --- The name of the topology
* `container` (required) --- Container number
* `path` (required) --- Path to the file relative to the directory where heron-controller is launched.
   Paths are not allowed to start with a `/` or contain a `..`.
* `offset` (required) --- Offset from the beggining of the file.
* `length` (required) --- Number of bytes to be returned.

***

### /topologies/metrics

The response JSON is a map of all the requested
(or if nothing is mentioned, all) components
of the topology, to the metrics that are reported
by that component.

Note that these metrics come from TMaster, which only holds metrics
for last 3 hours minutely data, and cumulative values. So if the `interval`
is greater than `10800` seconds, the values will be for all-time metrics.

#### Parameters

* `cluster` (required) --- The cluster in which the topology is running
* `environ` (required) --- The environment in which the topology is running
* `topology` (required) --- The name of the topology
* `component` (required) --- Component name
* `metricname` (required, repeated) --- Names of metrics to fetch
* `interval` (optional) --- For how many seconds, the metrics should be fetched for.
* `instance` (optional) --- IDs of the instances. If not present, return for all the instances.

***

### /topologies/metricstimeline

The response JSON is a map of all the requested
(or if nothing is mentioned, all) components
of the topology, to the metrics that are reported
by that component.

The difference between this and `/metrics` endpoint above, is that `/metrics` will report
cumulative value over the period of `interval` provided. On the other hand, `/metricstimeline`
endpoint will report minutely values for each metricname for each instance.

Note that these metrics come from Tmaster, which only holds metrics
for last 3 hours minutely data, and cumulative all-time values. So if the starttime
is older than 3 hours ago, those minutes would not be part of the response.

#### Parameters

* `cluster` (required) --- The cluster in which the topology is running
* `environ` (required) --- The environment in which the topology is running
* `topology` (required) --- The name of the topology
* `component` (required) --- Component name
* `metricname` (required, repeated) --- Names of metrics to fetch
* `starttime` (required) --- Start time for the metrics
* `endtime` (required) --- End time for the metrics
* `instance` (optional) --- IDs of the instances. If not present, return for all the instances.

### /topologies/metricsquery

Executes the metrics query for the topology and returns the result in form of minutely timeseries.
A detailed description of query language is given [below](#metricsquery)

Note that these metrics come from Tmaster, which only holds metrics
for last 3 hours minutely data, and cumulative all-time values. So if the starttime
is older than 3 hours ago, those minutes would not be part of the response.

#### Parameters

* `cluster` (required) --- The cluster in which the topology is running
* `environ` (required) --- The environment in which the topology is running
* `topology` (required) --- The name of the topology
* `starttime` (required) --- Start time for the metrics
* `endtime` (required) --- End time for the metrics
* `query` (required) --- Query to be executed

***

### /topologies/exceptionsummary

Returns summary of the exceptions for the component of the topology.
Duplicated exceptions are combined together and shows the number of
occurances, first occurance time and latest occurance time.

#### Parameters

* `cluster` (required) --- The cluster in which the topology is running
* `environ` (required) --- The environment in which the topology is running
* `topology` (required) --- The name of the topology
* `component` (required) --- Component name
* `instance` (optional) --- IDs of the instances. If not present, return for all the instances.

***

### /topologies/exceptions

Returns all exceptions for the component of the topology.

#### Parameters

* `cluster` (required) --- The cluster in which the topology is running
* `environ` (required) --- The environment in which the topology is running
* `topology` (required) --- The name of the topology
* `component` (required) --- Component name
* `instance` (optional) --- IDs of the instances. If not present, return for all the instances.

***

### /topologies/pid

Returns the PID of the instance jvm process.

#### Parameters

* `cluster` (required) --- The cluster in which the topology is running
* `environ` (required) --- The environment in which the topology is running
* `topology` (required) --- The name of the topology
* `instance` (required) --- Instance ID

***

### /topologies/jstack

Returns the thread dump of the instance jvm process.

#### Parameters

* `cluster` (required) --- The cluster in which the topology is running
* `environ` (required) --- The environment in which the topology is running
* `topology` (required) --- The name of the topology
* `instance` (required) --- Instance ID

***

### /topologies/jmap

Issues the `jmap` command for the instance, and saves the result in a file.
Returns the path to the file that can be downloaded externally.

#### Parameters

* `cluster` (required) --- The cluster in which the topology is running
* `environ` (required) --- The environment in which the topology is running
* `topology` (required) --- The name of the topology
* `instance` (required) --- Instance ID

***

### /topologies/histo

Returns histogram for the instance jvm process.

#### Parameters

* `cluster` (required) --- The cluster in which the topology is running
* `environ` (required) --- The environment in which the topology is running
* `topology` (required) --- The name of the topology
* `instance` (required) --- Instance ID

***

### /machines

Returns JSON describing all currently machines that a topology is running on.

```bash
$ curl "http://heron-tracker-url/machines?topology=mytopology1&cluster=cluster1&environ=prod"
```

#### Parameters

* `cluster` (optional) --- The cluster. Response will contain machines for the topologies only
  for this cluster.
* `environ` (optional) --- The environment.
* `topology` (optional, repeated) --- Both `cluster` and `environ` are required if the
  `topology` parameter is present

***

### <a name="metricsquery">Metrics Query Language</a>

#### Terminology

1. Univariate Timeseries --- A timeseries is called univariate if there is only one set
of minutely data. For example, a timeseries representing the sums of a number of timeseries
would be a univariate timeseries.
2. Multivariate Timeseries --- A set of multiple timeseries is collectively called multivariate.
Note that these timeseries are associated with their instances.

#### Operators

##### TS

    TS(component1, *, __emit-count/stream1)

Time Series Operator. This is the basic operator that is responsible for getting metrics from tmaster.
Accepts a list of 3 elements:

1. componentName
2. instance - can be "*" for all instances, or a single instance ID
3. metricName - Full metric name with stream id if applicable

Returns a univariate time series in case of a single instance id given, otherwise a multivariate time series.

***

##### DEFAULT

    DEFAULT(0, TS(component1, *, __emit-count/stream1))      <-- If the second operator returns more than one timeline, so will the DEFAULT operator.

    DEFAULT(100.0, SUM(TS(component2, *, __emit-count/default)))     <-- Second operator can be any operator

Default Operator. This operator is responsible for filling missing values in the metrics timeline.
Must have 2 arguments

1. First one must be a numeric constant representing the number to fill the missing values with
2. Second one must be one of the operators, that return the metrics timeline

Returns a univariate or multivariate time series, based on what the second operator is.

***

##### SUM

    SUM(TS(component1, instance1, metric1), DEFAULT(0, TS(component1, *, metric2)))

Sum Operator. This operator is used to take sum of all argument time series. It can have any number of arguments,
each of which must be one of the following two types:

1. Numeric constants, which will fill in the missing values as well, or
2. Operator, which returns one or more timelines

Returns only a single timeline representing the sum of all time series for each timestamp.
Note that "instance" attribute is not there in the result.

***

##### MAX

    MAX(100, TS(component1, *, metric1))

Max Operator. This operator is used to find max of all argument operators for each individual timestamp.
Each argument must be one of the following types:

1. Numeric constants, which will fill in the missing values as well, or
2. Operator, which returns one or more timelines

Returns only a single timeline representing the max of all the time series for each timestamp.
Note that "instance" attribute is not there in the result.

***

##### PERCENTILE

    PERCENTILE(99, TS(component1, *, metric1))

Percentile Operator. This operator is used to find a quantile of all timelines retuned by the arguments, for each timestamp.
This is a more general type of query similar to MAX. Note that `PERCENTILE(100, TS...)` is equivalent to `Max(TS...)`.
Each argument must be either constant or Operators.
First argument must always be the required Quantile.

1. Quantile (first argument) - Required quantile. 100 percentile = max, 0 percentile = min.
2. Numeric constants will fill in the missing values as well,
3. Operator - which returns one or more timelines

Returns only a single timeline representing the quantile of all the time series for each timestamp. Note that "instance" attribute is not there in the result.

***

##### DIVIDE

    DIVIDE(TS(componet1, *, metrics1), 100)

Divide Operator.
Accepts two arguments, both can be univariate or multivariate. Each can be of one of the following types:

1. Numeric constant will be considered as a constant time series for all applicable timestamps, they will not fill the missing values
2. Operator - returns one or more timelines

Three main cases are:

1. When both operands are multivariate -
    1. Divide operation will be done on matching data, that is, with same instance id.
    2. If the instances in both the operands do not match, error is thrown.
    3. Returns multivariate time series, each representing the result of division on the two corresponding time series.
2. When one operand is univariate, and other is multivariate -
    1. This includes division by constants as well.
    2. The univariate operand will participate with all time series in multivariate.
    3. The instance information of the multivariate time series will be preserved in the result.
    4. Returns multivariate time series.
3. When both operands are univariate.
    1. Instance information is ignored in this case
    2. Returns univariate time series which is the result of division operation.

***

##### MULTIPLY

    MULTIPLY(10, TS(component1, *, metrics1))

Multiply Operator. Has same conditions as division operator. This is to keep the API simple.
Accepts two arguments, both can be univariate or multivariate. Each can be of one of the following types:

1. Numeric constant will be considered as a constant time series for all applicable timestamps, they will not fill the missing values
2. Operator - returns one or more timelines

Three main cases are:

1. When both operands are multivariate -
    1. Multiply operation will be done on matching data, that is, with same instance id.
    2. If the instances in both the operands do not match, error is thrown.
    3. Returns multivariate time series, each representing the result of multiplication
        on the two corresponding time series.
2. When one operand is univariate, and other is multivariate -
    1. This includes multiplication by constants as well.
    2. The univariate operand will participate with all time series in multivariate.
    3. The instance information of the multivariate time series will be preserved in the result.
    4. Returns multivariate timeseries.
3. When both operands are univariate.
    1. Instance information is ignored in this case
    2. Returns univariate timeseries which is the result of multiplication operation.

***

##### SUBTRACT

    SUBTRACT(TS(component1, instance1, metrics1), TS(componet1, instance1, metrics2))

    SUBTRACT(TS(component1, instance1, metrics1), 100)

Subtract Operator. Has same conditions as division operator. This is to keep the API simple.
Accepts two arguments, both can be univariate or multivariate. Each can be of one of the following types:

1. Numeric constant will be considered as a constant time series for all applicable timestamps, they will not fill the missing values
2. Operator - returns one or more timelines

Three main cases are:

1. When both operands are multivariate -
    1. Subtract operation will be done on matching data, that is, with same instance id.
    2. If the instances in both the operands do not match, error is thrown.
    3. Returns multivariate time series, each representing the result of subtraction
        on the two corresponding time series.
2. When one operand is univariate, and other is multivariate -
    1. This includes subtraction by constants as well.
    2. The univariate operand will participate with all time series in multivariate.
    3. The instance information of the multivariate time series will be preserved in the result.
    4. Returns multivariate time series.
3. When both operands are univariate.
    1. Instance information is ignored in this case
    2. Returns univariate time series which is the result of subtraction operation.

***

##### RATE

    RATE(SUM(TS(component1, *, metrics1)))

    RATE(TS(component1, *, metrics2))

Rate Operator. This operator is used to find rate of change for all timeseries.
Accepts a only a single argument, which must be an Operators which returns univariate or multivariate time series.
Returns univariate or multivariate time series based on the argument, with each timestamp value
corresponding to the rate of change for that timestamp.
