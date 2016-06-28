---
title: Aurora Cluster
---

Heron supports deployment on [Apache Aurora](http://aurora.apache.org/) out of
the box. A step by step guide on how to setup Heron with Apache Aurora locally 
can be found in [Setting up Heron with Aurora Cluster Locally on Linux](../aurora-local-setup). You can also run Heron on
a [local scheduler](../local). 

## How Heron on Aurora Works

Aurora doesn't have a Heron scheduler *per se*. Instead, when a topology is
submitted to Heron, `heron` cli interacts with Aurora to automatically deploy
all the [components](../../../../concepts/architecture) necessary to [manage
topologies](../../../heron-cli).

## ZooKeeper

To run Heron on Aurora, you'll need to set up a ZooKeeper cluster and configure
Heron to communicate with it. Instructions can be found in [Setting up
ZooKeeper](../../statemanagers/zookeeper).

## Hosting Binaries

To deploy Heron, the Aurora cluster needs access to the
Heron core binary, which can be hosted wherever you'd like, so long as
it's accessible to Aurora (for example in [Amazon
S3](https://aws.amazon.com/s3/) or using a local blob storage solution). You
can download the core binary from github or build it using the instructions
in [Creating a New Heron Release](../../../../developers/compiling#building-a-full-release-package).

Once your Heron binaries are hosted somewhere that is accessible to Aurora, you
should run tests to ensure that Aurora can successfully fetch them.

## Aurora Scheduler Configuration

To configure Heron to use Aurora scheduler, modify the `scheduler.yaml`
config file specific for the Heron cluster. The following must be specified
for each cluster:

* `heron.class.scheduler` --- Indicates the class to be loaded for Aurora scheduler.
You should set this to `com.twitter.heron.scheduler.aurora.AuroraScheduler`

* `heron.class.launcher` --- Specifies the class to be loaded for launching and
submitting topologies. To configure the Aurora launcher, set this to
`com.twitter.heron.scheduler.aurora.AuroraLauncher`

* `heron.package.core.uri` --- Indicates the location of the heron core binary package.
The local scheduler uses this URI to download the core package to the working directory.

* `heron.directory.sandbox.java.home` --- Specifies the java home to
be used when running topologies in the containers.

* `heron.scheduler.is.service` --- This config indicates whether the scheduler
is a service. In the case of Aurora, it should be set to `False`.

### Example Aurora Scheduler Configuration

```yaml
# scheduler class for distributing the topology for execution
heron.class.scheduler: com.twitter.heron.scheduler.aurora.AuroraScheduler

# launcher class for submitting and launching the topology
heron.class.launcher: com.twitter.heron.scheduler.aurora.AuroraLauncher

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
