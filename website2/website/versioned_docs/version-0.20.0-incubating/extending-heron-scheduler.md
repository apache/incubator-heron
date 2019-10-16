---
id: version-0.20.0-incubating-extending-heron-scheduler
title: Implementing a Custom Scheduler
sidebar_label: Custom Scheduler
original_id: extending-heron-scheduler
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

To run a Heron topology, you’ll need to set up a scheduler that is responsible 
for topology management. Note: one scheduler is managing only one topology, 
for the purpose of better isolation. Heron currently supports the following schedulers out of the box:

* [Aurora](schedulers-aurora-cluster)
* [Kubernetes](schedulers-k8s-by-hand)
* [Kubernetes-Helm](schedulers-k8s-with-helm)
* [Nomad](schedulers-nomad)
* [Local scheduler](schedulers-local)
* [Slurm scheduler](schedulers-slurm)

If you'd like to run Heron on a not-yet-supported system, such as
[Amazon ECS](https://aws.amazon.com/ecs/), you can create your own scheduler
using Heron's spi, as detailed in the
sections below.

Java is currently the only supported language for custom schedulers. This may
change in the future.

## Java Setup

In order to create a custom scheduler, you need to import the `heron-spi`
library into your project.

#### Maven

```xml
<dependency>
  <groupId>org.apache.heron</groupId>
  <artifactId>heron-spi</artifactId>
  <version>{{% heronVersion %}}</version>
</dependency>
```

#### Gradle

```groovy
dependencies {
  compile group: "org.apache.heron", name: "heron-spi", version: "{{% heronVersion %}}"
}
```

## Interfaces

Creating a custom scheduler involves implementing each of the following Java
interfaces:

Interface | Role | Examples
:-------- |:---- |:--------
[`IPacking`](/api/org/apache/heron/spi/packing/IPacking.html) | Defines the algorithm used to generate physical plan for a topology. | [RoundRobin](/api/org/apache/heron/packing/roundrobin/RoundRobinPacking.html)
[`ILauncher`](/api/org/apache/heron/spi/scheduler/ILauncher.html) | Defines how the scheduler is launched | [Aurora](/api/org/apache/heron/scheduler/aurora/AuroraLauncher.html), [local](/api/org/apache/heron/scheduler/local/LocalLauncher.html)
[`IScheduler`](/api/org/apache/heron/spi/scheduler/IScheduler.html) | Defines the scheduler object used to construct topologies | [local](/api/org/apache/heron/scheduler/local/LocalScheduler.html)
[`IUploader`](/api/org/apache/heron/spi/uploader/IUploader.html) | Uploads the topology to a shared location accessible to the runtime environment of the topology | [local](/api/org/apache/heron/uploader/localfs/LocalFileSystemUploader.html) [HDFS](/api/org/apache/heron/uploader/hdfs/HdfsUploader.html) [S3](/api/org/apache/heron/uploader/s3/S3Uploader.html)

Heron provides a number of built-in implementations out of box.

## Running the Scheduler

To run the a custom scheduler, the implementation of the interfaces above must be specified in the [config](deployment-configuration).
By default, the heron-cli looks for configurations under `${HERON_HOME}/conf/`. The location can be overridden using option `--config-path`. 
Below is an example showing the command for [topology
submission](user-manuals-heron-cli#submitting-a-topology):

```bash
$ heron submit [cluster-name-storing-your-new-config]/[role]/[env] \
    --config-path [config-folder-path-storing-your-new-config] \
    /path/to/topology/my-topology.jar \
    biz.acme.topologies.MyTopology 
```

The implementation for each of the interfaces listed above must be on Heron's
[classpath](https://docs.oracle.com/javase/tutorial/essential/environment/paths.html). 


