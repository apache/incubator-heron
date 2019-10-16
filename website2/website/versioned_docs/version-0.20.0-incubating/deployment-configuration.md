---
id: version-0.20.0-incubating-deployment-configuration
title: Configuring a Cluster
sidebar_label: Configuration
original_id: deployment-configuration
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

To setup a Heron cluster, you need to configure a few files. Each file configures
a component of the Heron streaming framework.

* **scheduler.yaml** --- This file specifies the required classes for launcher,
scheduler, and for managing the topology at runtime. Any other specific parameters
for the scheduler go into this file.

* **statemgr.yaml** --- This file contains the classes and the configuration for state manager.
The state manager maintains the running state of the topology as logical plan, physical plan,
scheduler state, and execution state.

* **uploader.yaml** --- This file specifies the classes and configuration for the uploader,
which uploads the topology jars to storage. Once the containers are scheduled, they will
download these jars from the storage for running.

* **heron_internals.yaml** --- This file contains parameters that control
how heron behaves. Tuning these parameters requires advanced knowledge of heron architecture and its
components. For starters, the best option is just to copy the file provided with sample
configuration. Once you are familiar with the system you can tune these parameters to achieve
high throughput or low latency topologies.

* **metrics_sinks.yaml** --- This file specifies where the run-time system and topology metrics
will be routed. By default, the `file sink` and `tmaster sink` need to be present. In addition,
`scribe sink` and `graphite sink` are also supported.

* **packing.yaml** --- This file specifies the classes for `packing algorithm`, which defaults
to Round Robin, if not specified.

* **client.yaml** --- This file controls the behavior of the `heron` client. This is optional.

# Assembling the Configuration

All configuration files are assembled together to form the cluster configuration. For example,
a cluster named `devcluster` that uses the Aurora for scheduler, ZooKeeper for state manager and
HDFS for uploader will have the following set of configurations.

## scheduler.yaml (for Aurora)

```yaml
# scheduler class for distributing the topology for execution
heron.class.scheduler: org.apache.heron.scheduler.aurora.AuroraScheduler

# launcher class for submitting and launching the topology
heron.class.launcher: org.apache.heron.scheduler.aurora.AuroraLauncher

# location of java
heron.directory.sandbox.java.home: /usr/lib/jvm/java-1.8.0-openjdk-amd64/

# Invoke the IScheduler as a library directly
heron.scheduler.is.service: False
```

## statemgr.yaml (for ZooKeeper)

```yaml
# zookeeper state manager class for managing state in a persistent fashion
heron.class.state.manager: org.apache.heron.statemgr.zookeeper.curator.CuratorStateManager

# zookeeper state manager connection string
heron.statemgr.connection.string:  "127.0.0.1:2181"

# path of the root address to store the state in zookeeper  
heron.statemgr.root.path: "/heron"

# create the zookeeper nodes, if they do not exist
heron.statemgr.zookeeper.is.initialize.tree: True
```

## uploader.yaml (for HDFS)
```yaml
# Directory of config files for hadoop client to read from
heron.uploader.hdfs.config.directory:              "/home/hadoop/hadoop/conf/"

# The URI of the directory for uploading topologies in the HDFS
heron.uploader.hdfs.topologies.directory.uri:      "hdfs:///heron/topology/"
```

## packing.yaml (for Round Robin)
```yaml
# packing algorithm for packing instances into containers
heron.class.packing.algorithm:    org.apache.heron.packing.roundrobin.RoundRobinPacking
```

## client.yaml (for heron cli)
```yaml
# should the role parameter be required
heron.config.role.required: false

# should the environ parameter be required
heron.config.env.required: false
```
