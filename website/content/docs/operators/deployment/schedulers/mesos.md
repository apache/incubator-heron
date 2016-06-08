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

Using the Mesos scheduler is similar to deploying Heron on other systems in
that you use the [Heron CLI](../../../heron-cli) to manage topologies. The
difference is in the configuration and [scheduler
overrides](../../../heron-cli#submitting-a-topology) that you provide when
you [submit a topology](../../../heron-cli#submitting-a-topology).

A set of default configurations are provided with Heron in the `conf/mesos` directory. 
The default configurations use Zookeeper based state manager. 

When a Heron topology is submitted, the Mesos scheduler accepts the resource offers required to run the job and starts
the Heron containers on these nodes. Mesos scheduler itself runs in a local sandbox.

## ZooKeeper

To run Heron on Mesos, you'll need to set up a ZooKeeper cluster and configure
Heron to communicate with it. Instructions can be found in [Setting up
ZooKeeper](../../statemanagers/zookeeper).

## Useful Configuration files

These are some of the useful configuration files found in `conf/mesos` directory.

#### scheduler.yaml

This configuration file specifies the scheduler implementation to use and 
properties for that scheduler. Make sure to have the following properties set to the following:

* `heron.class.scheduler`: com.twitter.heron.scheduler.mesos.MesosScheduler

* `heron.class.launcher`: com.twitter.heron.scheduler.mesos.MesosLauncher

* `heron.scheduler.is.service`: True

You are free to customize the following:

* `heron.local.working.directory` &mdash; The directory to be used as
  Heron's Mesos scheduler sandbox directory.

* `heron.scheduler.background` &mdash; Flag whether to start Mesos Scheduler 
  in the background or in the blocking process.
  
#### statemgr.yaml

This is the configuration for the state manager. Mesos scheduler will use it for topology management as well as for 
storing its own state.

* `heron.class.state.manager` &mdash; Specifies the state manager. 
   By default it uses the zk state manager. Refer the `conf/local/statemgr.yaml` for local
   based state manager configurations.

## Working with Topologies

Once you've set up ZooKeeper and generated a Mesos-accessible Heron release,
any machine that has the Heron CLI tool installed can be used to manage Heron
topologies (i.e. can submit topologies, activate and deactivate them, etc.).

### Example Submission Command 

Here is an example command to submit the MultiSpoutExclamationTopology that comes with Heron.

```bash
heron submit mesos/<role> HERON_HOME/heron/examples/heron-examples.jar com.twitter.heron.examples.MultiSpoutExclamationTopology Topology_name
```

**NOTE:** `<role>` here stands for the Mesos framework user. This is the name of the user, from which the commands on 
the Mesos slaves will be executed. 

### Example Kill Command 

To kill the topology you can use the kill command with the cluster name and topology name.

```bash
$ heron kill mesos Topology_name
```
