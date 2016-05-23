# Deploying Heron

Heron is designed to be run in clustered, scheduler-driven environments. It can
be run in a `multi-tenant` or `dedicated` clusters. Furthermore, Heron supports 
`multiple clusters` and a user can submit topologies to any of these clusters. Each
of the cluster can use `different scheduler`. 

In order to deploy Heron in a cluster, you need the following 

* `Scheduler` &mdash; Heron requires a scheduler to run its topologies. It can 
be deployed on an existing cluster running alongside other big data frameworks. 
Alternatively, it can be deployed on a cluster of its own. Heron currently 
supports several scheduler options out of the box:
  * [Aurora](schedulers/aurora)
  * [Local](schedulers/local)
  * [Mesos](schedulers/mesos)
  * [Slurm](schedulers/slurm)

* `State Manager` &mdash; Heron needs a centralized mechanism for keeping track of
the state for each of the topologies. A topology state includes its logical plan, 
physical plan, and execution state. Heron supports the following for state managers:
  * [Local File System] (statemanagers/localfs)
  * [Zookeeper] (statemanagers/zookeeper) 

* `Uploader` &mdash; Heron uses an uploader to distribute the topology jars across
servers that actually run them. Heron supports several uploaders such as 
  * [HDFS] (uploaders/hdfs)
  * [Local File System] (uploaders/localfs)
  * [Amazon S3] (uploaders/s3)

* `Metrics Sinks` &mdash; Heron collects several metrics during topology execution.
These metrics can be routed to appropriate sink for storage and offline analysis.
Currently, Heron supports multiple sinks

  * `File Sink`
  * `Graphite Sink`
  * `Scribe Sink`

To implement a new scheduler, see
[Implementing a Custom Scheduler](../../contributors/custom-scheduler).


