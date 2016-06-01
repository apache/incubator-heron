---
title: Local Cluster
---

In addition to out-of-the-box schedulers for
[Aurora](../aurora), Heron can also be deployed in a local environment, which
stands up a mock Heron cluster on a single machine. This can be useful for
experimenting with Heron's features, testing a wide variety of possible cluster
events, and so on.

One of two state managers can be used for coordination when deploying locally:

* [ZooKeeper](../../statemanagers/zookeeper)
* [Local File System](../../statemanagers/localfs)

**Note**: Deploying a Heron cluster locally is not to be confused with Heron's
[simulator mode](../../../../developers/simulator-mode). Simulator mode enables
you to run topologies in a cluster-agnostic JVM process for the purpose of
development and debugging, while the local scheduler stands up a Heron cluster
on a single machine.

## How Local Deployment Works

Using the local scheduler is similar to deploying Heron on other schedulers.
The [Heron] (../../../heron-cli) cli is used to deploy and manage topologies
as would be done using a distributed scheduler. The main difference is in
the configuration.

## Local Scheduler Configuration

To configure Heron to use local scheduler, specify the following in `scheduler.yaml`
config file.

* `heron.class.scheduler` --- Indicates the class to be loaded for local scheduler.
Set this to `com.twitter.heron.scheduler.local.LocalScheduler`

* `heron.class.launcher` --- Specifies the class to be loaded for launching
topologies. Set this to `com.twitter.heron.scheduler.local.LocalLauncher`

* `heron.scheduler.local.working.directory` --- Provides the working
directory for topology. The working directory is essentially a scratch pad where
topology jars, heron core release binaries, topology logs, etc are generated and kept.

* `heron.package.core.uri` --- Indicates the location of the heron core binary package.
The local scheduler uses this URI to download the core package to the working directory.

* `heron.directory.sandbox.java.home` --- Specifies the java home to
be used when running topologies in the containers. Set to `${JAVA_HOME}` to
use the value set in the bash environment variable $JAVA_HOME.

### Example Local Scheduler Configuration

```yaml
# scheduler class for distributing the topology for execution
heron.class.scheduler: com.twitter.heron.scheduler.local.LocalScheduler

# launcher class for submitting and launching the topology
heron.class.launcher: com.twitter.heron.scheduler.local.LocalLauncher

# working directory for the topologies
heron.scheduler.local.working.directory: ${HOME}/.herondata/topologies/${CLUSTER}/${TOPOLOGY}

# location of the core package
heron.package.core.uri: file://${HERON_DIST}/heron-core.tar.gz

# location of java - pick it up from shell environment
heron.directory.sandbox.java.home: ${JAVA_HOME}
```
