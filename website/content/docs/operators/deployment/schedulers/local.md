---
title: Local Cluster
---

In addition to out-of-the-box schedulers for [Mesos](../mesos) and
[Aurora](../aurora), Heron can also be deployed in a local environment, which
stands up a mock Heron cluster on a single machine. This can be useful for
experimenting with Heron's features, testing a wide variety of possible cluster
events, and so on.

When deploying locally, you can use one of two state managers for coordination:

* [ZooKeeper](../../statemanagers/zookeeper)
* [Local File System](../../statemanagers/localfs)

**Note**: Deploying a Heron cluster locally is not to be confused with Heron's
[simulated mode](../../developers/java/local-mode.html). Simulated mode enables 
you to run topologies in a cluster-agnostic JVM process for the purpose of 
development and debugging, while the local scheduler stands up a Heron cluster 
on a single machine.

## How Local Deployment Works

Using the local scheduler is similar to deploying Heron on other systems in
that you use the [Heron](../../../heron-cli) cli to manage topologies. The
difference is in the configuration. 

## Local Scheduler Configuration

You can instruct Heron to use local scheduler by modifying the `scheduler.yaml`
config file. You'll need to specify the following:

* `heron.class.scheduler` --- Indicates the class to be loaded for local scheduler.
You should set this to `com.twitter.heron.scheduler.local.LocalScheduler`

* `heron.class.launcher` --- This specifies the class to be loaded for launching 
topologies. You should set this to `com.twitter.heron.scheduler.local.LocalLauncher`

* `heron.scheduler.local.working.directory` --- This config provides the working 
directory for topology. The working directory is essentially a scratch pad where 
topology jars, heron core release binaries, topology logs, etc are generated and kept.

* `heron.package.core.uri` --- Indicates the location of the heron core binary package.
The local scheduler uses this URI to download the core package to the working directory.

* `heron.directory.sandbox.java.home` --- This is used to specify the java home to
be used when running topologies in the containers. You could use `${JAVA_HOME}` which
means pick up the value set in the bash environment variable $JAVA_HOME.

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
