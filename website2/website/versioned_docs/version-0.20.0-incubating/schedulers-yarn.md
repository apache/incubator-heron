---
id: version-0.20.0-incubating-schedulers-yarn
title: Apache Hadoop YARN Cluster (Experimental)
sidebar_label: YARN Cluster
original_id: schedulers-yarn
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

In addition to out-of-the-box schedulers for [Aurora](schedulers-aurora-cluster), Heron can also be deployed on a
YARN cluster with the YARN scheduler. The YARN scheduler is implemented using the
[Apache REEF](https://reef.apache.org/) framework.

**Key features** of the YARN scheduler:

* **Heterogeneous container allocation:** The YARN scheduler will request heterogeneous containers
from the YARN ResourceManager [RM](http://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html). In other words the topology will not request more resources than what is really needed.

* **Container reuse:** The REEF framework allows the YARN scheduler to retain containers
across events like topology restarts.

## Topology deployment on a YARN Cluster

Using the YARN scheduler is similar to deploying Heron on other clusters, i.e. using the
[Heron CLI](user-manuals-heron-cli).
This document assumes that the Hadoop yarn client is installed and configured.

Following steps are executed when a Heron topology is submitted:

1. The REEF client copies the `Heron Core package` and the `topology package` on the distributed file system.
1. It then starts the YARN Application Master (AM) for the topology.
1. The AM subsequently invokes the `Heron Scheduler` in the same process.
1. This is followed by container allocation for the topology's master and workers. As a result `N+2`
containers are allocated for each topology.

### Configuring the Heron client classpath

**Under 0.14.2 version (including 0.14.2)**

  1. Command `hadoop classpath` provides a list of jars needed to submit a hadoop job. Copy all jars to `HERON_INSTALL_DIR/lib/scheduler`.
     * Do not copy commons-cli jar if it is older than version 1.3.1.
  1. Create a jar containing core-site.xml and yarn-site.xml. Add this jar to `HERON_INSTALL_DIR/lib/scheduler` too.

**After 0.14.3 version released**

It is unnecessary to copy hadoop-classpath-jars to `HERON_INSTALL_DIR/lib/scheduler` like what 0.14.2 version requested. [#1245](https://github.com/apache/incubator-heron/issues/1245) added `extra-launch-classpath` arguments, which makes it easier and more convenient to submit a topology to YARN.

> **Tips**
>
>***No matter which version of Heron you are using, there is something user should pay attention to*** if you want to submit a topology to YARN.
>
>For `localfs-state-manager`
>
>* The version of common-cli jar should be greater than or equal to 1.3.1.
>
>For `zookeeper-state-manager`
>
>* The version of common-cli jar should be greater than or equal to 1.3.1.
>* The version of curator-framework jar should be greater than or equal to 2.10.0
>* The version of curator-client jar should be greater than or equal to 2.10.0

### Configure the YARN scheduler

A set of default configuration files are provided with Heron in the [conf/yarn](https://github.com/apache/incubator-heron/tree/master/heron/config/src/yaml/conf/yarn) directory.
The default configuration uses the local state manager. This will work with single-node local
YARN installation only. A Zookeeper based state management will be needed for topology
deployment on a multi-node YARN cluster.

1. Custom Heron Launcher for YARN: `YarnLauncher`
1. Custom Heron Scheduler for YARN: `YarnScheduler`
1. State manager for multi-node deployment:
`org.apache.heron.statemgr.zookeeper.curator.CuratorStateManager`
1. `YarnLauncher` performs the job of uploader also. So `NullUploader` is used.

## Topology management

### Topology Submission
**Command**

**Under 0.14.2 version (including 0.14.2)**

`$ heron submit yarn heron-api-examples.jar org.apache.heron.examples.api.AckingTopology AckingTopology`


**After 0.14.3 version released**

`$ heron submit yarn heron-api-examples.jar org.apache.heron.examples.api.AckingTopology AckingTopology --extra-launch-classpath <extra-classpath-value>`

>**Tips**
>
>1. More details for using the `--extra-launch-classpath` argument in 0.14.3 version. It supports both a single directory which including all `hadoop-lib-jars` and multiple directories separated by colon such as what `hadoop classpath` gives. ***The submit operation will fail if any path is invalid or if any file is missing.***
>2. if you want to submit a topology to a specific YARN queue, you can set the `heron.scheduler.yarn.queue` argument in `--config-property`. For instance, `--config-property heron.scheduler.yarn.queue=test`. This configuration could be found in the [conf/yarn/scheduler](https://github.com/apache/incubator-heron/blob/master/heron/config/src/yaml/conf/yarn/scheduler.yaml) file too. `default` would be the YARN default queue as YARN provided.

**Sample Output**

```bash
INFO: Launching topology 'AckingTopology'
...
...
Powered by
     ___________  ______  ______  _______
    /  ______  / /  ___/ /  ___/ /  ____/
   /     _____/ /  /__  /  /__  /  /___
  /  /\  \     /  ___/ /  ___/ /  ____/
 /  /  \  \   /  /__  /  /__  /  /
/__/    \__\ /_____/ /_____/ /__/

...
...
org.apache.heron.scheduler.yarn.ReefClientSideHandlers INFO:  Topology AckingTopology is running, jobId AckingTopology.
```

**Verification**

Visit the YARN http console or execute command `yarn application -list` on a yarn client host.

```bash
Total number of applications (application-types: [] and states: [SUBMITTED, ACCEPTED, RUNNING]):1
                Application-Id	    Application-Name	    Application-Type	      User	     Queue	             State	       Final-State	       Progress	                       Tracking-URL
application_1466548964728_0004	      AckingTopology	                YARN	     heron	   default	           RUNNING	         UNDEFINED	             0%	                                N/A
```

### Topology termination
**Command**

`$ heron kill yarn AckingTopology`


### Log File location

Assuming HDFS as the file system, Heron logs and REEF logs can be found in the following locations:

1. Logs generated when the topologies AM starts:
`<LOG_DIR>/userlogs/application_1466548964728_0004/container_1466548964728_0004_01_000001/driver.stderr`

1. Ths scheduler's logs are created on the first/AM container:
`<NM_LOCAL_DIR>/usercache/heron/appcache/application_1466548964728_0004/container_1466548964728_0004_01_000001/log-files`

1. Logs generated when the TMaster starts in its container:
`<LOG_DIR>/userlogs/application_1466548964728_0004/container_1466548964728_0004_01_000002/evaluator.stderr`

1. The TMaster's logs are created on the second container owned by the topology app:
`<NM_LOCAL_DIR>/usercache/heron/appcache/application_1466548964728_0004/container_1466548964728_0004_01_000002/log-files`

1. Worker logs are created on the remaining containers in the YARN NodeManager's local directory.


## Work in Progress

1. The YARN Scheduler will restart any failed workers and TMaster containers. However [AM HA](https://hadoop.apache.org/docs/r2.7.1/hadoop-yarn/hadoop-yarn-site/ResourceManagerHA.html)  is not
 supported yet. As a result AM failure will result in topology failure.
 Issue: [#949](https://github.com/apache/incubator-heron/issues/949)
1. TMaster and Scheduler are started in separate containers. Increased network latency can result
 in warnings or failures. Issue: [#951](https://github.com/apache/incubator-heron/issues/951)
