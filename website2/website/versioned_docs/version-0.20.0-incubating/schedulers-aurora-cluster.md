---
id: version-0.20.0-incubating-schedulers-aurora-cluster
title: Aurora Cluster
sidebar_label: Aurora Cluster
original_id: schedulers-aurora-cluster
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

Heron supports deployment on [Apache Aurora](http://aurora.apache.org/) out of
the box. A step by step guide on how to setup Heron with Apache Aurora locally 
can be found in [Setting up Heron with Aurora Cluster Locally on Linux](schedulers-aurora-local). You can also run Heron on
a [local scheduler](schedulers-local). 

## How Heron on Aurora Works

Aurora doesn't have a Heron scheduler *per se*. Instead, when a topology is
submitted to Heron, `heron` cli interacts with Aurora to automatically deploy
all the [components](heron-architecture) necessary to [manage
topologies](user-manuals-heron-cli).

## ZooKeeper

To run Heron on Aurora, you'll need to set up a ZooKeeper cluster and configure
Heron to communicate with it. Instructions can be found in [Setting up
ZooKeeper](state-managers-zookeeper).

## Hosting Binaries

To deploy Heron, the Aurora cluster needs access to the
Heron core binary, which can be hosted wherever you'd like, so long as
it's accessible to Aurora (for example in [Amazon
S3](https://aws.amazon.com/s3/) or using a local blob storage solution). You
can download the core binary from github or build it using the instructions
in [Creating a New Heron Release](compiling-overview#building-all-components).

Command for fetching the binary is in the `heron.aurora` config file. By default it is 
using a `curl` command to fetch the binary. For example, if the binary is hosted in 
HDFS, you need to change the fetch user package command in `heron.aurora` to use the 
`hdfs` command instead of `curl`.

### `heron.aurora` example binary fetch using HDFS

```bash
fetch_heron_system = Process(
  name = 'fetch_heron_system',
  cmdline = 'hdfs dfs -get %s %s && tar zxf %s' % (heron_core_release_uri, 
        core_release_file, core_release_file)
)
```

Once your Heron binaries are hosted somewhere that is accessible to Aurora, you
should run tests to ensure that Aurora can successfully fetch them.

## Uploading the Topologies

Heron uses an uploader to upload the topology to a shared location so that a worker can fetch 
the topology to its sandbox. The configuration for an uploader is in the `uploader.yaml` 
config file. For distributed Aurora deployments, Heron can use `HdfsUploader` or `S3Uploader`. 
Details on configuring the uploaders can be found in the documentation for the 
[HDFS](uploaders-hdfs) and [S3](uploaders-amazon-s3) uploaders. 

After configuring an uploader, the `heron.aurora` config file needs to be modified accordingly to 
fetch the topology. 

### `heron.aurora` example topology fetch using HDFS

```bash
fetch_user_package = Process(
  name = 'fetch_user_package',
  cmdline = 'hdfs dfs -get %s %s && tar zxf %s' % (heron_topology_jar_uri, 
          topology_package_file, topology_package_file)
)
```

## Aurora Scheduler Configuration

To configure Heron to use Aurora scheduler, modify the `scheduler.yaml`
config file specific for the Heron cluster. The following must be specified
for each cluster:

* `heron.class.scheduler` --- Indicates the class to be loaded for Aurora scheduler.
You should set this to `org.apache.heron.scheduler.aurora.AuroraScheduler`

* `heron.class.launcher` --- Specifies the class to be loaded for launching and
submitting topologies. To configure the Aurora launcher, set this to
`org.apache.heron.scheduler.aurora.AuroraLauncher`

* `heron.package.core.uri` --- Indicates the location of the heron core binary package.
The local scheduler uses this URI to download the core package to the working directory.

* `heron.directory.sandbox.java.home` --- Specifies the java home to
be used when running topologies in the containers.

* `heron.scheduler.is.service` --- This config indicates whether the scheduler
is a service. In the case of Aurora, it should be set to `False`.

### Example Aurora Scheduler Configuration

```yaml
# scheduler class for distributing the topology for execution
heron.class.scheduler: org.apache.heron.scheduler.aurora.AuroraScheduler

# launcher class for submitting and launching the topology
heron.class.launcher: org.apache.heron.scheduler.aurora.AuroraLauncher

# location of the core package
heron.package.core.uri: file:///vagrant/.herondata/dist/heron-core-release.tar.gz

# location of java - pick it up from shell environment
heron.directory.sandbox.java.home: /usr/lib/jvm/java-1.8.0-openjdk-amd64/

# Invoke the IScheduler as a library directly
heron.scheduler.is.service: False
```

## Working with Topologies

After setting up ZooKeeper and generating an Aurora-accessible Heron core binary
release, any machine that has the `heron` cli tool can be used to manage Heron
topologies (i.e. can submit topologies, activate and deactivate them, etc.).

The most important thing at this stage is to ensure that `heron` cli is available
across all machines. Once the cli is available, Aurora as a scheduler
can be enabled by specifying the proper configuration when managing topologies.
