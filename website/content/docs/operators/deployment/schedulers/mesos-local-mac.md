---
title: Setting up Heron with Mesos Cluster Locally on Mac
---

This is a step by step guide to run Heron on Mesos cluster locally.

## Get Heron with Mesos Scheduler Implementation
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

Run the command to build and install Heron:

```bash
bazel run --config=darwin --verbose_failures -- scripts/packages:heron-client-install.sh --user
```
This will build everything into `$HOME/.heron`, including all the configurations. In future releases Mesos Scheduler will be included and you will not need to rebuild Heron from source. Then you can add `--config-path=your_conf_path` to your `heron submit` command to tell Heron where to find your config files (default value is `$HOME/.heron/conf`). 


## Run Topology in Mesos

After setting up Heron and Mesos, let's submit a topology to test if everything works right, Run:

```bash
heron submit mesos --verbose ~/.heron/examples/heron-examples.jar com.twitter.heron.examples.ExclamationTopology ExclamationTopology
```

If the topology is submitted successfully, you will see:

```bash
[2016-07-25 22:04:41 -0700] com.twitter.heron.scheduler.mesos.MesosLauncher INFO:  For checking the status and logs of the topology, use the working directory $HOME/.herondata/topologies/mesos/$USER/ExclamationTopology
[2016-07-25 22:04:41 -0700] com.twitter.heron.scheduler.SubmitterMain FINE:  Topology ExclamationTopology submitted successfully
INFO: Topology 'ExclamationTopology' launched successfully
INFO: Elapsed time: 4.114s.
``` 

Note that this doesn't necessarily mean it's successfully launched in Mesos, to check that you can go to the working directory as shown in the output. You will see:

* `heron-examples.jar`: the jar which contains the topology submitted.
* `heron-conf`: configurations used to launch the topology.
* `log-files`: directory containing Mesos scheduler's log.

You can look at the log file to see whether the launch succeeded. If it succeeded, at the end of the log file you'll see:

```bash
[2016-07-25 22:15:47 -0700] com.twitter.heron.scheduler.mesos.framework.MesosFramework INFO:  Received status update [...]
[2016-07-25 22:15:47 -0700] com.twitter.heron.scheduler.mesos.framework.MesosFramework INFO:  Task with id 'container_1_1469510147073:0' RUNNING
``` 

If the launch fails, you will see the error message. E.g. if the Heron can't find Mesos library in the given path, it will complain:

```bash
[2016-07-25 22:04:42 -0700] stderr STDERR:  Failed to load native Mesos library from /usr/lib/mesos/0.28.1/lib
[2016-07-25 22:04:42 -0700] stderr STDERR:  Exception in thread "main"
[2016-07-25 22:04:42 -0700] stderr STDERR:  java.lang.UnsatisfiedLinkError: no mesos in java.library.path
[2016-07-25 22:04:42 -0700] stderr STDERR:      at java.lang.ClassLoader.loadLibrary(ClassLoader.java:1867)
...
```

## Mesos Management Console

Another way to check your topology is running is to look at the Mesos management console, if the topology was launched successfully you should see two containers of the topology running:

![result page](/img/mesos-management-console-with-topology.png)

They are containers of topology master and heron container. If you click the `sandbox` on the right side, you can access the logs of these processes. E.g. the sandbox of heron container looks as below:

![container-container-sandbox](/img/container-container-sandbox.png)

If you go to `log-files` directory, you will see application and GC log of the processes run in this container:

![container-log-files](/img/container-log-files.png)

If you click `container_1_exclaim1_1.log.0`, you will see the bolt log of the ExclamationTopology:

![bolt-log](/img/bolt-log.png)

## Heron UI

If you have Heron tools installed (see [Quick Start Guide](../../../../getting-started)), you can also monitor topology through `heron-ui`. First you need to start the `heron-tracker`. To start the tracker, modify the value of `statemgrs.rootpath` in `$HOME/.herontools/conf/heron_tracker.yaml` to `$HOME/.herondata/repository/state/mesos`. This tells Heron tracker to go to `mesos` directory (default is `local`) to find topology state information. Now start `heron-tracker`:

```bash
$ heron-tracker
... Running on port: 8888
... Using config file: $HOME/.herontools/conf/heron_tracker.yaml
```

Then start `heron-ui`:

```bash
$ heron-ui
... Running on port: 8889
... Using tracker url: http://localhost:8888
```

Now go to the UI ([http://localhost:8889](http://localhost:8889)) and you'll see your topology:

![mesos-local-heron-ui](/img/mesos-local-heron-ui.png)

Click on the topology, you'll see its metrics:

![mesos-local-heron-ui-more](/img/mesos-local-heron-ui-more.png)

If you click on the `job` button, you'll enter the Mesos Management Console page:

![mesos-local-heron-ui-to-mesos-console](/img/mesos-local-heron-ui-to-mesos-console.png)

## Kill Topology

To kill the topology, run:

```bash
heron kill mesos ExclamationTopology
```
