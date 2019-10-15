---
id: version-0.20.0-incubating-schedulers-mesos-local-mac
title: Setting up Heron with Mesos Cluster Locally on Mac
sidebar_label: Mesos Cluster Locally
original_id: schedulers-mesos-local-mac
---
<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at
      http://www.apache.org/licenses/LICENSE-2.0
    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

This is a step by step guide to run Heron on a Mesos cluster locally.

## Install Heron
Follow [Quick Start Guide](getting-started-local-single-node) to install Heron.

## Setting up an Apache Mesos Cluster Locally

Follow [Installing Mesos on your Mac with Homebrew]
(https://mesosphere.com/blog/2014/07/07/installing-mesos-on-your-mac-with-homebrew/)
to install and run Mesos. To confirm Mesos cluster is ready for accepting Heron topologies, access
the Mesos management console [http://localhost:5050](http://localhost:5050) and confirm there is
activated slaves.

![console page](assets/mesos-management-console.png)

## Configure Heron

### State Manager
By default, Heron uses Local File System State Manager on Mesos to manage states. Modify
`$HOME/.heron/conf/mesos/statemgr.yaml` to use ZooKeeper. For more details see [Setting up
ZooKeeper](state-managers-zookeeper).

### Scheduler
Heron needs to know where to load the lib to interact with Mesos. Change the config
`heron.mesos.native.library.path` in `$HOME/.heron/conf/mesos/scheduler.yaml` to the library path
of the Mesos install. If Mesos is installed through `brew`, the library path should be 
`/usr/local/Cellar/mesos/your_mesos_version/lib`.

> Mesos only offers a C++ interface, which is not portable across platforms.


## Run Topology in Mesos

After setting up Heron and Mesos, submit a topology using the following command. By default this
command loads the config in `$HOME/.heron/conf`. Add `--config-path=your_conf_path` to change the
config path.

```bash
heron submit mesos --verbose ~/.heron/examples/heron-api-examples.jar \
org.apache.heron.examples.api.ExclamationTopology ExclamationTopology
```

The following will be displayed upon a successful submit.

```bash
[2016-07-25 22:04:41 -0700] org.apache.heron.scheduler.mesos.MesosLauncher INFO: \
For checking the status and logs of the topology, use the working directory \
$HOME/.herondata/topologies/mesos/$USER/ExclamationTopology
[2016-07-25 22:04:41 -0700] org.apache.heron.scheduler.SubmitterMain FINE:  Topology \
ExclamationTopology submitted successfully
INFO: Topology 'ExclamationTopology' launched successfully
INFO: Elapsed time: 4.114s.
``` 

Note that this doesn't necessarily mean the topology is successfully launched in Mesos, to verify
check the working directory as shown in the output. You will see:

* `heron-examples.jar`: the jar which contains the topology submitted.
* `heron-conf`: configurations used to launch the topology.
* `log-files`: directory containing Mesos scheduler's log.

The log file will show whether the launch succeeded. If it succeeded, at the end of the log file
it will show the task is running.

```bash
[2016-07-25 22:15:47 -0700] org.apache.heron.scheduler.mesos.framework.MesosFramework INFO: \
Received status update [...]
[2016-07-25 22:15:47 -0700] org.apache.heron.scheduler.mesos.framework.MesosFramework INFO: \
Task with id 'container_1_1469510147073:0' RUNNING
``` 

If the launch fails, an error message will be included. For example, if the Mesos library isn't
found in the configured location, the following exception will occur.

```bash
[2016-07-25 22:04:42 -0700] stderr STDERR:  Failed to load native Mesos library from \
/usr/lib/mesos/0.28.1/lib
[2016-07-25 22:04:42 -0700] stderr STDERR:  Exception in thread "main"
[2016-07-25 22:04:42 -0700] stderr STDERR:  java.lang.UnsatisfiedLinkError: no mesos in \ 
java.library.path
[2016-07-25 22:04:42 -0700] stderr STDERR:      at \
java.lang.ClassLoader.loadLibrary(ClassLoader.java:1867)
...
```

## Mesos Management Console

Another way to check your topology is running is to look at the Mesos management console. If it
was launched successfully, two containers will be running.

![result page](assets/mesos-management-console-with-topology.png)

To view the process logs, click the `sandbox` on the right side. The sandbox of the heron container
is shown below.

![container-container-sandbox](assets/container-container-sandbox.png)

The `log-files` directory includes the application and GC log of the processes running in this
container.

![container-log-files](assets/container-log-files.png)

The bolt log of the ExclamationTopology is `container_1_exclaim1_1.log.0`. Below is a sample of it.

![bolt-log](assets/bolt-log.png)

## Heron UI

Install Heron tools to monitor the topology with the `heron-ui` (see [Quick Start Guide]
(../../../../getting-started)). Configure the value of `statemgrs.rootpath` in 
`$HOME/.herontools/conf/heron_tracker.yaml` to `$HOME/.herondata/repository/state/mesos` before
starting the tracker. This configuration sets the location of the state manager root path. Start
tracker and the UI.

```bash
$ heron-tracker
... Running on port: 8888
... Using config file: $HOME/.herontools/conf/heron_tracker.yaml
```

```bash
$ heron-ui
... Running on port: 8889
... Using tracker url: http://localhost:8888
```

Go to the UI at [http://localhost:8889](http://localhost:8889) to see the topology.

![mesos-local-heron-ui](assets/mesos-local-heron-ui.png)

To see the metrics, click on the topology.

![mesos-local-heron-ui-more](assets/mesos-local-heron-ui-more.png)

To enter the Mesos Management Console page, click the `job` button.

![mesos-local-heron-ui-to-mesos-console](assets/mesos-local-heron-ui-to-mesos-console.png)

## Kill Topology

To kill the topology, run:

```bash
heron kill mesos ExclamationTopology
```
