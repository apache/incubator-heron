---
title: The Heron Tracker REST API
---

The **Heron Tracker** is a web service that continuously gathers a wide
variety of information about Heron topologies in your cluster(s) and exposes
that information through a JSON REST API.  More on the role of the Tracker can
be found [here](../../concepts/architecture#heron-tracker).

The Tracker can run within your Heron cluster (e.g.
[Mesos](../../operators/deployment/schedulers/mesos) or [Aurora](../../operators/deployment/schedulers/aurora)) or
outside of it, provided that the machine on which it runs has access to your
Heron cluster.

## Starting the Tracker

You can start the Heron Tracker by running the `heron-tracker` executable, which
you can generate when you [compile Heron](../../developers/compiling).

```bash
$ cd /path/to/heron/binaries
$ ./heron-tracker
```

By default, the Tracker runs on port 8888. You can specify a different port
using the `--port` flag:

```bash
$ ./heron-tracker --port=1234
```

## JSON Interface

All Heron Tracker endpoints return a JSON object with the following information:

* `status` &mdash; One of the following: `success`, `failure`.
* `executiontime` &mdash; The time it took to return the HTTP result, in seconds.
* `message` &mdash; Some endpoints return special messages in this field for certain
  requests. Often, this field will be an empty string.
* `result` &mdash; The result payload of the request. The contents will depend on
  the endpoint.
* `version` &mdash; The Heron release version used to build the currently running
  Tracker executable.

## Endpoints

* `/` (redirects to `/topologies`)
* [`/machines`](#-machines)
* [`/topologies`](#-topologies)
* [`/topologies/states`](#-topologies-states)
* [`/topologies/info`](#-topologies-info)
* [`/topologies/logicalplan`](#-topologies-logicalplan)
* [`/topologies/physicalplan`](#-topologies-physicalplan)
* [`/topologies/executionstate`](#-topologies-executionstate)
* [`/topologies/metrics`](#-topologies-metrics)
* [`/topologies/metricstimeline`](#-topologies-metricstimeline)
* [`/topologies/metricsquery`](#-topologies-metricsquery)
* [`/topologies/exceptionsummary`](#-topologies-exceptionsummary)
* [`/topologies/pid`](#-topologies-pid)
* [`/topologies/jstack`](#-topologies-jstack)
* [`/topologies/jmap`](#-topologies-jmap)
* [`/topologies/histo`](#-topologies-histo)

All of these endpoints are documented in the sections below.

***

### `/machines`

Returns JSON describing all currently available machines, sorted by (1) data
center (if you're running Heron in multiple data centers), (2) environment, and
(3) topology.

#### Example Request

```bash
$ curl http://heron-tracker-url/machines
```

#### Optional parameters

* `dc` &mdash; The data center. If the data center you provide is valid, the JSON
  payload will list machines only in that data center. You will receive a 404
  if the data center is invalid. Example:

  ```bash
  $ curl "http://heron-tracker-url/machines?dc=datacenter1"
  ```

* `environ` &mdash; The environment. Must be either `devel` or `prod`, otherwise you
  will receive a 404. Example:

  ```bash
  $ curl "http://heron-tracker-url/machines?environ=devel"
  ```

* `topology` (repeated) &mdash; Both `dc` and `environ` are required if the
  `topology` parameter is present

  ```bash
  $ curl "http://heron-tracker-url/machines?topology=mytopology1&dc=datacenter1&environ=prod"
  ```

#### Response

The value of the `result` field should look something like this:

```json
{
  <dc1>: {
    <environ1>: {
      <topology1>: [machine1, machine2, ...],
      <topology2>: [...],
    },
    <environ2> : {...},
    ...
  },
  <dc2>: {...}
}
```

***

### `/topologies`

Returns JSON describing all currently available topologies

#### Optional Parameters

* `dc` &mdash; The data center. If the data center you provide is valid, the JSON
  payload will list topologies only in that data center. You will receive a 404
  if the data center is invalid. Example:

  ```bash
  $ curl "http://heron-tracker-url/topologies?dc=datacenter1"
  ```

* `environ` &mdash; Lists topologies by the environment in which they're running.
  Example:

  ```bash
  $ curl "http://heron-tracker-url/topologies?environ=prod"
  ```

#### Response

The value of the `result` field should look something like this:

```json
{
  <dc1>: {
    <environ1>: [
      topology1,
      topology2,
      ...
    ],
    <environ2>: [...],
  },
  <dc2>: {...}
}
```

***

### `/topologies/states`

The current execution state of topologies in a cluster. Topologies can be
grouped by data center, environment, or both.

#### Optional Parameters

* `dc` &mdash; The data center. If the data center you provide is valid, the JSON
  payload will list topologies only in that data center. You will receive a 404
  if the data center is invalid. Example:

  ```bash
  $ curl "http://heron-tracker-url/topologies/states?dc=datacenter1"
  ```

* `environ` &mdash; Lists topologies by the environment in which they're running.
  Example:

  ```bash
  $ curl "http://heron-tracker-url/topologies/states?environ=prod"
  ```

#### Response

The value of the `result` field should look something like this:

```json
{
  <dc1>: {
    <environ1>: {
      <topology1>: {
        <execution state>
      },
      <topology2>: {...},      
      ...
    },
    <environ2>: {...{,
    ...
  <dc2>: {...}
}
```

Each execution state object lists the following:

* `release_username` &mdash; The user that generated the Heron release for the
  topology
* `has_tmaster_location` &mdash; Whether the topology's Topology Master
  currently has a location
* `release_tag` &mdash; This is a legacy
* `uploader_version` &mdash; TODO
* `dc` &mdash; The data center in which the topology is running
* `jobname` &mdash; TODO
* `release_version` &mdash; TODO
* `environ` &mdash; The environment in which the topology is running
* `submission_user` &mdash; The user that submitted the topology
* `submission_time` &mdash; The time at which the topology was submitted
  (timestamp in milliseconds)
* `role` &mdash; TODO
* `has_physical_plan` &mdash; Whether the topology currently has a physical plan

***

### `/topologies/info`

#### Required Parameters

* `dc` &mdash; The data center in which the topology is running
* `environ` &mdash; The environment in which the topology is running
* `topology` &mdash; The name of the topology

#### Example Request

```bash
$ curl "http://heron-tracker-url/topologies/info?dc=datacenter1&environ=prod&topology=user_topology_1"
```

#### Response

The value of the `result` field should lists the following:

* `name` &mdash; The name of the topology
* `tmaster_location` &mdash; Information about the machine on which the topology's
  Topology Master (TM) is running, including the following: the controller port, the
  host, the master port, the stats port, and the ID of the TM.
* `physical_plan` &mdash; A JSON representation of the physical plan of the
  topology, which includes configuration information for the topology as well
  as information about all current spouts, bolts, state managers, and
  instances.
* `logical_plan` &mdash; A JSON representation of the logical plan of the topology,
  which includes information about all of the spouts and bolts in the topology.
* `execution_state` &mdash; The execution state of the topology. For more on
  execution state, see the section regarding the `/topologies/states` endpoint
  above.

***

### `/topologies/logicalplan`

Returns a JSON object for the [logical
plan](../../concepts/topologies#logical-plan) of a topology.

#### Required Parameters

* `dc` &mdash; The data center in which the topology is running
* `environ` &mdash; The environment in which the topology is running
* `topology` &mdash; The name of the topology

#### Example Request

```bash
$ curl "http://heron-tracker-url/topologies/logicalplan?dc=datacenter1&environ=prod&topology=user_topology_1"
```

#### Response

The value of the `result` field should look something like this:

```json
TODO
```

* `spouts` &mdash; A set of JSON objects representing each spout in the topology.
  The following information is listed for each spout:
  * `source` &mdash; The source of tuples for the spout.
  * `version` &mdash; The Heron release version for the topology.
  * `type` &mdash; The type of the spout, e.g. `kafka`, `kestrel`, etc.
  * `outputs` &mdash; A list of streams to which the spout outputs tuples.
* `bolts` &mdash; A set of JSON objects representing each bolt in the topology.
  * `outputs` &mdash; A list of outputs for the bolt.
  * `inputs` &mdash; A list of inputs for the bolt.

***

### `/topologies/physicalplan`

Returns a JSON object for the [physical
plan](../../concepts/topologies#physical-plan) of a topology.

#### Required Parameters

* `dc` &mdash; The data center in which the topology is running
* `environ` &mdash; The environment
* `topology` &mdash; The name of the topology

#### Example Request

```bash
$ curl "http://heron-tracker-url/topologies/physicalplan?dc=datacenter1&environ=prod&topology=user_topology_1"
```

#### Response



***

### `/topologies/executionstate`

The current execution state of a given topology.

#### Required Parameters

* `dc` &mdash; The data center in which the topology is running
* `environ` &mdash; The environment in which the topology is running
* `topology` &mdash; The name of the topology

#### Example Request

```bash
$ curl "http://heron-tracker-url/topologies/executionstate?dc=datacenter1&environ=prod&topology=user_topology_1"
```

#### Response

The value of the `result` field will be a JSON object akin to the one
documented in a [section above](#-topologies-states).

***

### `/topologies/metrics`

***

### `/topologies/metricstimeline`

***

### `/topologies/metricsquery`

***

### `/topologies/exceptionsummary`

***

### `/topologies/pid`

***

### `/topologies/jstack`

***

### `/topologies/jmap`

#### Required Parameters

* `dc` &mdash; The data center in which the topology is running
* `environ` &mdash; The environment in which the topology is running
* `topology` &mdash; The name of the topology
* `instance` &mdash; The instance ID of the desired Heron instance

#### Response

***

### `/topologies/histo`

Returns JSON containing a histogram

#### Required Parameters

* `dc` &mdash; The data center
* `environ` &mdash; The environment
* `topology` &mdash; The name of the topology
* `instance` &mdash; The instance ID of the desired Heron instance

#### Response

The `result` field should look something like this:

```json
{
  "command": "<command executed at server>",
  "stdout": "<text from stdout from executing the command>",
  "stderr": "<text from stderr from executing the command>"
}
```
