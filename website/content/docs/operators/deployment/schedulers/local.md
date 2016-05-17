# Local Deployment

In addition to out-of-the-box schedulers for [Mesos](../mesos) and
[Aurora](../aurora), Heron can also be deployed in a local environment, which
stands up a mock Heron cluster on a single machine. This can be useful for
experimenting with Heron's features, testing a wide variety of possible cluster
events, and so on.

When deploying locally, you can use one of two coordination mechanisms:

1. A locally-running [ZooKeeper](#zookeeper)
2. [The local filesystem](#local-filesystem)

**Note**: Deploying a Heron cluster locally is not to be confused with Heron's
[local mode](../../developers/java/local-mode.html). Local mode enables you to run
topologies in a cluster-agnostic JVM process for the purpose of development and
debugging, while the local scheduler stands up a Heron cluster on a single
machine.

## How Local Deployment Works

Using the local scheduler is similar to deploying Heron on other systems in
that you use the [Heron CLI](../../../heron-cli) to manage topologies. The
difference is in the configuration and [scheduler
overrides](../../../heron-cli#submitting-a-topology) that you provide when
you [submit a topology](../../../heron-cli#submitting-a-topology).

### Required Scheduler Overrides

For the local scheduler, you'll need to provide the following scheduler
overrides:

* `heron.local.working.directory` &mdash; The local directory to be used as
  Heron's sandbox directory.
* `state.manager.class` &mdash; This will depend on whether you want to use
  [ZooKeeper](#zookeeper) or the [local filesystem](#local-filesystem) for
  coordination.

For info on scheduler overrides, see the documentation on using the [Heron
CLI](../../../heron-cli).

### Optional Scheduler Overrides

The `heron.core.release.package` parameter is optional. It specifies the path to
a local TAR file for the `core` component of the desired Heron release. Assuming
that you've built a full [Heron release](../../../../developers/compiling#building-a-full-release-package), this TAR will be
located by default at `bazel-genfiles/release/heron-core-unversioned.tar`,
relative to the root of your Heron repository. If you set
`heron.core.release.package`, Heron will update all local binaries in Heron's
working directory; if you don't set `heron.core.release.package`, Heron will use
the binaries already contained in Heron's working directory.

### CLI Flags

In addition to setting scheduler overrides, you'll need to set the following
[CLI flags](../../../heron-cli):

* `--config-file` &mdash; This flag needs to point to the `local_scheduler.conf`
  file in `heron/cli/src/python/local_scheduler.conf`.
* `--config-loader` &mdash; You should set this to
  `com.twitter.heron.scheduler.util.DefaultConfigLoader`.

## ZooKeeper

To run the local scheduler using ZooKeeper for coordination, you'll need to set
the following scheduler overrides:

* `state.manager.class` should be set to
  `com.twitter.heron.state.curator.CuratorStateManager`
* `zk.connection.string` should specify a ZooKeeper connection string, such as
  `localhost:2818`.
* `state.root.address` should specify a root
  [ZooKeeper node](https://zookeeper.apache.org/doc/trunk/zookeeperOver.html#Nodes+and+ephemeral+nodes)
  for Heron, such as `/heron`.

### Example Submission Command for ZooKeeper

```bash
$ heron-cli submit \
    "heron.local.working.directory=/Users/janedoe/heron-sandbox \
    state.manager.class=com.twitter.heron.state.curator.CuratorStateManager \
    zk.connection.string=localhost:2181 \
    state.root.address=/heron" \
    /Users/janedoe/topologies/topology1.jar \
    biz.acme.topologies.TestTopology \
    --config-file=/Users/janedoe/heron/cli/src/python/local_scheduler.conf \
    --config-loader=com.twitter.heron.scheduler.util.DefaultConfigLoader     
```

## Local Filesystem

To run the local scheduler using your machine's filesystem for coordination,
you'll need to set the following scheduler override:

* `state.manager.class` should be set to
  `com.twitter.heron.state.localfile.LocalFileStateManager`.

### Example Submission Command for Local Filesystem

```bash
$ heron-cli submit \
    "heron.local.working.directory=/Users/janedoe/heron-sandbox \
    state.manager.class=com.twitter.heron.state.localfile.LocalFileStateManager" \
    /Users/janedoe/topologies/topology1.jar \
    biz.acme.topologies.TestTopology \
    --config-file=/Users/janedoe/heron/cli/src/python/local_scheduler.conf \
    --config-loader=com.twitter.heron.scheduler.util.DefaultConfigLoader    
```
