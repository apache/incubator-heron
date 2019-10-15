---
id: version-0.20.0-incubating-deployment-overview
title: Deployment Overiew
sidebar_label: Deployment Overiew
original_id: deployment-overview
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

Heron is designed to be run in clustered, scheduler-driven environments. It can
be run in a `multi-tenant` or `dedicated` clusters. Furthermore, Heron supports
`multiple clusters` and a user can submit topologies to any of these clusters. Each
of the cluster can use `different scheduler`. A typical Heron deployment is shown
in the following figure.

<br />
![Heron Deployment](assets/heron-deployment.png)
<br/>

A Heron deployment requires several components working together. The following must
be deployed to run Heron topologies in a cluster:

* **Scheduler** --- Heron requires a scheduler to run its topologies. It can
be deployed on an existing cluster running alongside other big data frameworks.
Alternatively, it can be deployed on a cluster of its own. Heron currently
supports several scheduler options:
  * [Aurora](schedulers-aurora-cluster)
  * [Local](schedulers-local)
  * [Slurm](schedulers-slurm)
  * [YARN](schedulers-yarn)
  * [Kubernetes By Hand](schedulers-k8s-by-hand)
  * [Kubernetes with Helm](schedulers-k8s-with-helm)

* **State Manager** --- Heron state manager tracks the state of all deployed
topologies. The topology state includes its logical plan,
physical plan, and execution state. Heron supports the following state managers:
  * [Local File System](state-managers-local-fs)
  * [Zookeeper](state-managers-zookeeper)

* **Uploader** --- The Heron uploader distributes the topology jars to the
servers that run them. Heron supports several uploaders
  * [HDFS](uploaders-hdfs)
  * [Local File System](uploaders-local-fs)
  * [Amazon S3](uploaders-amazon-s3)

* **Metrics Sinks** --- Heron collects several metrics during topology execution.
These metrics can be routed to a sink for storage and offline analysis.
Currently, Heron supports the following sinks

  * `File Sink`
  * `Graphite Sink`
  * `Scribe Sink`

* **Heron Tracker** --- Tracker serves as the gateway to explore the topologies.
It exposes a REST API for exploring logical plan, physical plan of the topologies and
also for fetching metrics from them.

* **Heron UI** --- The UI provides the ability to find and explore topologies visually.
UI displays the DAG of the topology and how the DAG is mapped to physical containers
running in clusters. Furthermore, it allows the ability to view logs, take heap dump, memory
histograms, show metrics, etc.
