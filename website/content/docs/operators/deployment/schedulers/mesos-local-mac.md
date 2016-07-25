---
title: Setting up Heron with Mesos Cluster Locally on Mac
---

This is a step by step guide to run Heron on Mesos cluster locally.

## Install Heron
At the time this article is written, Mesos scheduler is not included in the release version. So you need the latest version of Heron to run it on Mesos. Follow [Compiling](../../../../developers/compiling/compiling) to make sure you can build from the latest version.

## Setting up Apache Mesos Cluster Locally

Follow [Installing Mesos on your Mac with Homebrew](https://mesosphere.com/blog/2014/07/07/installing-mesos-on-your-mac-with-homebrew/) to have Mesos installed and running. To confirm Mesos cluster are ready for accepting Heron topologies, make sure you can access Mesos management console [http://localhost:5050](http://localhost:5050) and see activated slaves.

![console page](/img/mesos-management-console.png)

## Configure Heron

### State Manager
By default, Heron uses Local File System State Manager on Mesos to manage states. You can modify `heron/config/src/yaml/conf/mesos/statemgr.yaml` to make it use ZooKeeper. For more details please see [Setting up ZooKeeper](../statemanagers/zookeeper).

### Scheduler
Heron needs to know where to load the lib to interact with Mesos. Change the config `heron.mesos.native.library.path` in `heron/config/src/yaml/conf/mesos/scheduler.yaml` to the library path of your Mesos. If you installed Mesos through `brew`, the library path should be `/usr/local/Cellar/mesos/your_mesos_version/lib`.

> Mesos only offers interface in C++ library, which is not portable across platforms. So Heron has to ask you for the path of the library, which is able to run in your platform. 

## Compile Heron

Run the command to build and install Heron to pick up all the configuration changes:

```
bazel run --config=darwin --verbose_failures -- scripts/packages:heron-client-install.sh --user
```

## Run Topology in Mesos

After setting up Heron and Mesos, let's submit a topology to test if everything works right, Run:

```
heron submit mesos --verbose ~/.heron/examples/heron-examples.jar com.twitter.heron.examples.ExclamationTopology ExclamationTopology
```

The output shows if the topology has been launched successfully. Check Mesos management console, you should see the topology:

![result page](/img/mesos-management-console-with-topology.png)

To kill the topology, run:

```
heron kill mesos ExclamationTopology
```
