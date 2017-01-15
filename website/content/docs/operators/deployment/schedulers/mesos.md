---
title: Mesos (Work in Progress)
---

Heron supports deployment on [Apache Mesos](http://mesos.apache.org/). 
Heron can also run on Mesos using [Apache Aurora](../aurora) as
a scheduler or using a [local scheduler](../local).

## How Heron on Mesos Works

Heron's Mesos scheduler interacts with Mesos to stand up all of the
[components](../../../../concepts/architecture) necessary to [manage
topologies](../../../heron-cli).

## ZooKeeper

To run Heron on Mesos, you'll need to set up a ZooKeeper cluster and configure
Heron to communicate with it. Instructions can be found in [Setting up
ZooKeeper](../../statemanagers/zookeeper).

## Hosting Binaries

In order to deploy Heron, your Mesos cluster will need to have access to a
variety of Heron binaries, which can be hosted wherever you'd like, so long as
it's accessible to Mesos (for example in [Amazon S3](https://aws.amazon.com/s3/)
or using a local blog storage solution). You can build those binaries using the
instructions in [Creating a New Heron Release](../../../../developers/compiling#building-a-full-release-package).

Once your Heron binaries are hosted somewhere that's accessible to Mesos, you
should run tests to ensure that Mesos can successfully fetch them.

**Note**: Setting up a Heron cluster involves changing a series of configuration
files in the actual `heron` repository itself (for example [ZooKeeper
configuration](#ZooKeeper)). You should build Heron releases for deployment only
*after* you've made the necessary configuration changes.

### Specifying a URI for Heron Releases

Once you've set up a way to host Heron release binaries, you should modify the
Mesos configuration in `heron/cli2/src/python/mesos_scheduler.conf`. In
particular, you can specify a URI using the `heron.core.release.uri` parameter.
Here's an example:

```
heron.core.release.uri:http://s3.amazonaws.com/my-heron-binaries
```

## Working with Topologies

Once you've set up ZooKeeper and generated a Mesos-accessible Heron release,
any machine that has the `heron-cli` tool can be used to manage Heron
topologies (i.e. can submit topologies, activate and deactivate them, etc.).

The most important thing at this stage is to ensure that `heron-cli` is synced
across all machines that will be [working with topologies](../../../heron-cli).
Once that has been ensured, you can use Mesos as a scheduler by specifying the
proper configuration and configuration loader when managing topologies.

### Specifying a Configuration

You'll need to specify a scheduler configuration at all stages of a topology's
[lifecycle](../../../../concepts/topologies#topology-lifecycle) by using the
`--config-file` flag to point at a configuration file. There is a default Mesos
configuration located in the Heron repository at
`heron/cli/src/python/mesos_scheduler.conf`. You can use this file as is,
modify it, or use an entirely different configuration.

Here's an example CLI command using this configuration:

```bash
$ heron-cli activate \
    # Set scheduler overrides, point to a topology JAR, etc.
    --config-file=/Users/janedoe/heron/heron/cli/src/python/mesos_scheduler.conf` \
    # Other parameters
```

### Specifying the Configuration Loader

You can use Heron's Mesos configuration loader by setting the
`--config-loader` flag to `com.twitter.heron.scheduler.aurora.MesosConfigLoader`.
Here's an example CLI command:

```bash
$ heron-cli submit \
    # Set scheduler overrides, point to a topology JAR, etc.
    --config-loader=com.twitter.heron.scheduler.mesos.MesosConfigLoader \
    # Other parameters
```
