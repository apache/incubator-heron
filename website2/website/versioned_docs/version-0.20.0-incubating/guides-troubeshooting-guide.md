---
id: version-0.20.0-incubating-guides-troubeshooting-guide
title: Topology Troubleshooting Guide
sidebar_label: Topology Troubleshooting Guide
original_id: guides-troubeshooting-guide
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

### Overview

This guide provides basic steps to troubleshoot a topology.
These are starting steps to troubleshoot potential issues and identify root causes easily.

This guide is organized into following broad sections:

* [Determine topology running status and health](#running)
* [Identify topology problems](#problem)
* [Frequently seen issues](#frequent)

This guide is useful for topology developers. Issues related to Heron configuration setup or
its [internal architecture](heron-architecture), like `schedulers`, etc, are discussed in Configuration and Heron Developers respectively, and not discussed here.

<a name="running"></a>

### Determine topology running status and health

#### 1. Estimate your data rate

It is important to estimate how much data a topology is expected to consume.
A useful approach is to begin by estimating a data rate in terms of items per minute. The emit count (tuples per minute) of each spout should match the data rate for the corresponding data
stream. If spouts are not consuming and emitting the data at the same rate as it
is produced, this is called `spout lag`.

Some spouts, like `Kafka Spout` have a lag metric that can be
directly used to measure health. It is recommended to have some kind of lag
metric for a custom spout, so that it's easier to check and create monitoring alerts.

#### 2. Absent Backpressure

Backpressure initiated by an instance means that the concerned instance is not
able to consume data at the same rate at which it is being receiving. This
results in all spouts getting clamped (they will not consume any more data)
until the backpressure is relieved by the instance.

Backpressure is measured in milliseconds per minute, the time an instance was under backpressure.  For example, a value of 60,000 means an instance was under backpressure for the whole minute (60 seconds).

A healthy topology should not have backpressure. Backpressure usually results in the
spout lag build up since spouts get clamped, but it should not be considered as
a cause, only a symptom.  

Therefore, adjust and iterate Topology until backpressure is absent.

#### 3. Absent failures

Failed tuples are generally considered bad for a topology, unless it is a required feature (for instance, lowest possible latency is needed at the expense of possible dropped tuples). If
`acking` is disabled, or even when enabled and not handled properly in spouts,
this can result in data loss, without adding spout lag.


<a name="problem"></a>
### Identify topology problems

#### 1. Look at instances under backpressure

Backpressure metrics identifies which instances have been under backpressure. Therefore, jump directly to the logs of that instance to see what is going wrong with the
instance. Some of the known causes of backpressure are discussed in the [frequently seen issues](#frequent) section below.

#### 2. Look at items pending to be acked

Spouts export a metric which is a sampled value of the number of tuples
still in flight in the topology. Sometimes, `max-spout-pending` config limits
the consumption rate of the topology. Increasing that spout's parallelism
generally solves the issue.

<a name="frequent"></a>

### Frequently seen issues

#### 1. Topology does not launch

*Symptom* - Heron client fails to launch the topology.

Note that heron client will execute the topology's `main` method on the local
system, which means spouts and bolts get instantiated locally, serialized, and then
sent over to schedulers as part of `topology.defn`. It is important to make sure
that:

1. All spouts and bolts are serializable.
2. Don't instantiate a non-serializable attribute in constructor. Leave those to
   a bolt's `prepare` or a spout's `open` method, which gets called during start
   time of the instances.
3. The `main` method should not try to access anything that your local machine
   may not have access to.

#### 2. Topology does not start

We assume here that heron client has successfully launched the topology.

*Symptom* - Physical plan or logical plan does not show up on UI

*Possible Cause* - One of more of stream managers have not yet connected to
Tmaster.

*What to do* -

1. Go to the Tmaster logs for the topology. The zeroth container is reserved for
   Tmaster. Go to the container and browse to

        log-files/heron-tmaster-<topology-name><topology-id>.INFO

    and see which stream managers have not yet connected. The `stmgr` ID
    corresponds to the container number. For example, `stmgr-10` corresponds to
    container 10, and so on.

2. Visit that container to
    see what is wrong in stream manager's logs, which can be found in `log-files`
    directory similar to Tmaster.

#### 3. Instances are not starting up

A topology would not start until all the instances are running. This may be a cause of a topology not starting.

*Symptom* - The stream manager logs for that instance never showed that the
instance connected to it.

*Possible Cause* - Bad configs being passed when the instance process was
getting launched.

*What to do* -

1. Visit the container and browse to `heron-executor.stdout` and
   `heron-executor.stderr` files. All commands to instantiate the instances and
   stream managers are redirected to these files.

2. Check JVM configs for anything amiss.

3. If `Xmx` is too low, increase `containerRAM` or `componentRAM`. Note that
   because heron sets aside some RAM for its internal components, like stream
   manager and metrics manager, having a large number of instances and low
   `containerRAM` may starve off these instances.

#### 4. Metrics for a component are missing/absent

*Symptom* - The upstream component is emitting data, but this component is not
executing any, and no metrics are being reported.

*Possible Cause* - The component might be stuck in a deadlock. Since one
instance is a single JVM process and user code is called from the main thread,
it is possible that execution is stuck in `execute` method.

*What to do* -

1. Check logs for one of the concerned instances. If `open` (in a spout) or
   `prepare` (in a bolt) method is not completed, check the code logic to see
   why the method is not completed.

2. Check the code logic if there is any deadlock in a bolt's `execute` or a
   spout's `nextTuple`, `ack` or `fail` methods. These methods should be
   non-blocking.

#### 5. There is backpressure from internal bolt

Bolts are called internal if it does not talk to any external service. For example,
the last bolt might be talking to some database to write its results, and would
not be called an internal bolt.

This is invariably due to lack of resources given to this bolt. Increasing
parallelism or RAM (based on code logic) can solve the issue.

#### 6. There is backpressure from external bolt

By the same definition as above, an external bolt is the one which is accessing
an external service. It might still be emitting data downstream.

*Possible Cause 1* - External service is slowing down this bolt.

*What to do* -

1. Check if the external service is the bottleneck, and see if adding resources
   to it can solve it.

2. Sometimes, changing bolt logic to tune caching vs write rate can make a
   difference.

*Possible Cause 2* - Resource crunch for this bolt, just like an internal bolt
above.

*What to do* -

1. This should be handled in the same was as internal bolt - by increasing the
   parallelism or RAM for the component.

#### 7. Debugging Java topologies.
The jar containing the code for building the topology, along with the spout and bolt 
code, is deployed in the containers. A Heron Instance is started in each container, 
with each Heron Instance responsible for running a bolt or a spout. One way to debug 
Java code is to write debug logs to the log files for tracking and debugging purposes.

Logging is the preferred mode for debugging as it makes it easer to find issues in both 
the short and long term in the topology. If you want to perform step-by-step debugging 
of a JVM process, however, this can be achieved by enabling remote debugging for the Heron Instance.

Follow these steps to enable remote debugging:

1. Add the java options to enable debuggin on all the Heron Instances that will be started.
   This can be achieved by adding the options ```-agentlib:jdwp=transport=dt_socket,address=8888,server=y,suspend=n```. Here's an example:

    ```java
    conf.setDebug(true);
    conf.setMaxSpoutPending(10);
    conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS, "-XX:+HeapDumpOnOutOfMemoryError");
    conf.setComponentJvmOptions("word",
           "-agentlib:jdwp=transport=dt_socket,address=8888,server=y,suspend=n");
    conf.setComponentJvmOptions("exclaim1",
           "-agentlib:jdwp=transport=dt_socket,address=8888,server=y,suspend=n");
    ```

2. Use the steps as given in the tutorial to setup remote debugging eith eclipse.
   [set up Remote Debugging in Eclipse](http://help.eclipse.org/neon/index.jsp?topic=%2Forg.eclipse.jdt.doc.user%2Ftasks%2Ftask-remotejava_launch_config.htm) . 
   To setup remote debugging with intelij use [remote debugging instructions](https://www.jetbrains.com/help/idea/2016.2/run-debug-configuration-remote.html) .
 
3. Once the topology is activated start the debugger at ```localhost:{port}``` if in standalone
   local deployment or ``` {IP}/{hostname}:{port}``` for multi container remote deployment. And you will be able to debug the code step by step.