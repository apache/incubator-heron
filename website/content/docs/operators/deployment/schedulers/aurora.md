---
title: Aurora
---

Heron supports deployment on [Apache Aurora](http://aurora.apache.org/) out of
the box. You can also run Heron on [Apache Mesos](../mesos) directly or using
a [local scheduler](../local).

## How Heron on Aurora Works

Aurora doesn't have a Heron scheduler *per se*. Instead, when a topology is
submitted to Heron, `heron-cli` interacts with Aurora to automatically stand up
all the [components](../../../../concepts/architecture) necessary to [manage
topologies](../../../heron-cli).

## ZooKeeper

To run Heron on Aurora, you'll need to set up a ZooKeeper cluster and configure
Heron to communicate with it. Instructions can be found in [Setting up
ZooKeeper](../../statemanagers/zookeeper).

## Hosting Binaries

In order to deploy Heron, your Aurora cluster will need to have access to a
variety of Heron binaries, which can be hosted wherever you'd like, so long as
it's accessible to Aurora (for example in [Amazon
S3](https://aws.amazon.com/s3/) or using a local blob storage solution). You can
build those binaries using the instructions in [Creating a New Heron
Release](../../../../developers/compiling#building-a-full-release-package).

Once your Heron binaries are hosted somewhere that is accessible to Aurora, you
should run tests to ensure that Aurora can successfully fetch them.

**Note**: Setting up a Heron cluster involves changing a series of configuration
files in the actual `heron` repository itself (documented in the sections
below). You should build a Heron release for deployment only *after* you've made
those changes.

## Working with Topologies

Once you've set up ZooKeeper and generated an Aurora-accessible Heron release,
any machine that has the `heron-cli` tool can be used to manage Heron topologies
(i.e. can submit topologies, activate and deactivate them, etc.).

The most important thing at this stage is to ensure that `heron-cli` is synced
across all machines that will be [working with topologies](../../../heron-cli).
Once that has been ensured, you can use Aurora as a scheduler by specifying the
proper configuration and configuration loader when managing topologies.

### Specifying a Configuration

You'll need to specify a scheduler configuration at all stages of a topology's
[lifecycle](../../../../concepts/topologies#topology-lifecycle) by using the
`--config-file` flag to point at a configuration file. There is a default Aurora
configuration located in the Heron repository at
`heron/cli/src/python/aurora_scheduler.conf`. You can use this file as is,
modify it, or use an entirely different configuration.

Here's an example CLI command using this configuration:

```bash
$ heron-cli activate \
    # Set scheduler overrides, point to a topology JAR, etc.
    --config-file=/Users/janedoe/heron/heron/cli/src/python/aurora_scheduler.conf` \
    # Other parameters
```

### Specifying the Configuration Loader

You can use Heron's Aurora configuration loader by setting the
`--config-loader` flag to `com.twitter.heron.scheduler.aurora.AuroraConfigLoader`.
Here's an example CLI command:

```bash
$ heron-cli submit \
    # Set scheduler overrides, point to a topology JAR, etc.
    --config-loader=com.twitter.heron.scheduler.aurora.AuroraConfigLoader \
    # Other parameters
```
