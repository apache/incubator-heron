## Topology Troubleshooting Guide

### Overview

This guide aims to provide some basic steps at troubleshooting your topology.
These server as the starting steps to troubleshoot any issues with the topology,
and leave you in a position to identify the root cause easily.

This guide is organized into following broad sections:

* How to tell if my topology is running fine
* Where is the problem in my topology
* Frequently seen issues

This guide is useful for topology writers. The issues related to Heron setup or
its internals, like `schedulers`, etc, are not discussed here.

### How to tell if my topology is running fine

#### 1. Know your data rate

It is expected that you know how much data your topology is expected to consume.
Try to estimate it in terms of items per minute. The emit count (tuples per
minute) of each spout should match the data rate for the corresponding data
stream. If spouts are not consuming and emitting the data at the same rate as it
is produced, we call this scenario `spout lag`.

Some spouts, like `Kafka Spout (TODO: Add link)` have a lag metric that can be
directly used to measure health. It is recommended to have some kind of lag
metric if you have a custom spout, so that its easier to check, as well as can
be used to set up monitoring alerts.

#### 2. No Backpressure

Backpressure initated by an instance means that the concerned instance is not
able to consume data at the same rate at which it is being receiving. This
results in all spouts getting clamped (they will not consume any more data)
until the backpressure is releived by the instance.

It is measured in milliseconds per minute, an instance was under backpressure.
A value of 60,000 means an instance was under backpressure for the whole minute.

A healthy topology should not have backpressure. This usually results in the
spout lag build up since spouts get clamped, but it should not be considered as
a cause, only a symptom.

#### 3. No failures

Failed tuples are considered bad for a topology, unless its a feature. If
`acking` is disabled, or even when enabled and not handled properly in spouts,
this can result in data loss, without adding spout lag.

### Where is the problem in my topology

#### 1. Look at the instance under backpressure

The metric directly shows which instances have been under backpressure. You can
jump directly to the logs (TODO: insert UI image for accessing logs) of that
instance to see what is going wrong with the instance. Some of the known causes
of backpressure are being discussed below.

#### 2. Look at items pending to be acked

The spouts export a metric which is a sampled value of the number of tuples
still in flight in the topology. Sometimes, `max-spout-pending` config limits
the consumption rate of the topology. Increasing that spout's parallelism
generally solves the issue.

### Frequently seen issues

#### 1. I can not launch the topology

*Symptom* - Heron client fails to launch the topology.

Note that heron client will execute the topology's `main` method on your local
system, which means spouts and bolts get instantiated here, serialized, and then
sent over to schedulers as part of `topology.defn`. It is important to make sure
that:

1. All spouts and bolts are serializable.
2. Don't instantiate a non-serializable attribute in constructor. Leave those to
   a bolt's `prepare` or a spout's `open` method, which gets called during start
   time of the instances.
3. The `main` method should not try to access anything that your local machine
   may not have access to.

#### 2. My topology does not start

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

A topology would not start until all the instances are running. So you may see
this as a cause of topology not getting started.

*Symptom* - The stream manager logs for that instance never showed that the
instance connected to it.

*Possible Cause* - Bad configs being passed when the instance process was
getting launched.

*What to do* -

1. Visit the container and browse to `heron-executor.stdout` and
   `heron-executor.stderr` files. All commands to instantiate the instances and
   stream managers are redirected to these files.

2. Check jvm configs for anything amiss.

3. If `Xmx` is too low, increase `containerRAM` or `componentRAM`. Note that
   because heron sets aside some RAM for its internal components, like stream
   manager and metrics manager, having a large number of instances and low
   `containerRAM` may starve off these instances.

#### 4. I do not see any metrics for a component

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

We call a bolt internal if it does not talk to any external servive. For example,
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
