---
id: version-0.20.0-incubating-schedulers-aurora-local
title: Setting up Heron with Aurora Cluster Locally on Linux
sidebar_label: Aurora Locally
original_id: schedulers-aurora-local
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

It is possible to setup Heron with a locally running Apache Aurora cluster.
This is a step by step guide on how to configure and setup all the necessary
components.

## Setting Up Apache Aurora Cluster locally

You first need to setup Apache Aurora locally. More detailed description of the
following steps can be found in [A local Cluster with Vagrant](http://aurora.apache.org/documentation/latest/getting-started/vagrant/)

### Step 1: Install VirtualBox and Vagrant

Download and install VirtualBox and Vagrant on your machine. If vagrant is successfully
installed in your machine the following command should list several common commands
for this tool

```bash
$ vagrant
```

### Step 2: Clone the Aurora repository

You can get the source repository for Aurora with the following command

```bash
$ dgit clone git://git.apache.org/aurora.git
```

Once the clone is complete cd into the aurora folder

```bash
$ cd aurora
```

### Step 3: Starting Local Aurora Cluster

To start the local cluster all you have to do is execute the following command. It will install all the needed dependencies like Apache Mesos and Zookeeper in the VM.

```bash
$ vagrant up
```

Additionally to get rid of some of the warning messages that you get during up command execute the following command

```bash
$ vagrant plugin install vagrant-vbguest
```

You can verify that the Aurora cluster is properly running by opening the following links in your web-browser

* Scheduler - http://192.168.33.7:8081
* Observer - http://192.168.33.7:1338
* Mesos Master - http://192.168.33.7:5050
* Mesos Agent - http://192.168.33.7:5051

If you go into http://192.168.33.7:8081/scheduler you can notice that the name of the default cluster that is setup in aurora is
named `devcluster` this will be important to note when submitting typologies from heron.

![Heron topology](assets/aurora-local-cluster-start.png)

## Installing Heron within the Cluster VM

Now that the Aurora cluster is setup you need to install heron within the cluster VM in order to be able to get the Heron
deployment working. Since this is a fresh VM instance you will have to install the basic software such as "unzip" and set
the JAVA_HOME path as an environmental variable ( Just need to add this to .bashrc file). After you have the basic stuff
working follow the following steps to install Heron in the VM. You can ssh into the VM with the following command

```bash
$ vagrant ssh
```

### Step 1.a : Download installation script files

You can download the script files that match your Linux distribution from
https://github.com/apache/incubator-heron/releases/tag/{{% heronVersion %}}

For example for the {{% heronVersion %}} release the files you need to download For Ubuntu will be the following.

* `heron-install-{{% heronVersion %}}-ubuntu.sh`

Optionally - You want need the following for the steps in the blog post

* `heron-api-install-{{% heronVersion %}}-ubuntu.sh`
* `heron-core-{{% heronVersion %}}-ubuntu.tar.gz`

### Step 1.b: Execute the client and tools shell scripts


```bash
$ chmod +x heron-install-VERSION-PLATFORM.sh
$ ./heron-install-VERSION-PLATFORM.sh --user
Heron client installer
----------------------

Uncompressing......
Heron is now installed!

Make sure you have "/home/vagrant/bin" in your path.
```

After this you need to add the path "/home/vagrant/bin". You can just execute the following command
or add it to the end of  .bashrc file ( which is more convenient ).

```bash
$ export PATH=$PATH:/home/vagrant/bin
```

Install the following packages to make sure that you have all the needed dependencies in the VM.
You might have to do sudo apt-get update before you execute the following.

```bash
$ sudo apt-get install git build-essential automake cmake libtool zip libunwind-setjmp0-dev zlib1g-dev unzip pkg-config -y
```

## Configuring State Manager ( Apache Zookeeper )

Since Heron only uses Apache Zookeeper for coordination the load on the Zookeeper
node is minimum. Because of this it is sufficient to use a single Zookeeper node or
if you have an Zookeeper instance running for some other task you can simply use that.
Since Apache Aurora already uses an Zookeeper instance you can directly use that instance
to execute State Manager tasks of Heron. First you need to configure Heron to work with
the Zookeeper instance. You can find meanings of each attribute in [Setting Up ZooKeeper
State Manager](state-managers-zookeeper). Configurations for State manager are
located in the directory `/home/vagrant/.heron/conf/aurora`.

Open the file `statemgr.yaml` using vim ( or some other text editor you prefer )
and add/edit the file to include the following.

```yaml
# local state manager class for managing state in a persistent fashion
heron.class.state.manager: org.apache.heron.statemgr.zookeeper.curator.CuratorStateManager

# local state manager connection string
heron.statemgr.connection.string:  "127.0.0.1:2181"

# path of the root address to store the state in a local file system
heron.statemgr.root.path: "/heronroot"

# create the zookeeper nodes, if they do not exist
heron.statemgr.zookeeper.is.initialize.tree: True

# timeout in ms to wait before considering zookeeper session is dead
heron.statemgr.zookeeper.session.timeout.ms: 30000

# timeout in ms to wait before considering zookeeper connection is dead
heron.statemgr.zookeeper.connection.timeout.ms: 30000

# timeout in ms to wait before considering zookeeper connection is dead
heron.statemgr.zookeeper.retry.count: 10

# duration of time to wait until the next retry
heron.statemgr.zookeeper.retry.interval.ms: 10000
```

## Creating Paths in Zookeeper

Next you need to create some paths within Zookeeper since some of the paths
are not created by Heron automatically. So you need to create them manually.
Since Aurora installation already installed Zookeeper, you can use the Zookeeper
cli to create the manual paths.

```bash
$ sudo ./usr/share/zookeeper/bin/zkCli.sh
```

This will connect to the Zookeeper instance running locally. Then execute the
following commands from within the client to create paths `/heronroot/topologies`
and `/heron/topologies`. Later in "Associating new Aurora cluster into Heron UI"
you will see that you only need to create `/heronroot/topologies` but for now lets
create both to make sure you don't get any errors when you run things.

```bash
create /heronroot null
create /heronroot/topologies null
```

```bash
create /heron null
create /heron/topologies null
```

## Configuring Scheduler ( Apache Aurora )

Next you need to configure Apache Aurora to be used as the Scheduler for our Heron
local cluster. In order to do this you need to edit the `scheduler.yaml` file that is
also located in `/home/vagrant/.heron/conf/aurora`. Add/Edit the file to include the
following. More information regarding parameters can be found in [Aurora Cluster](schedulers-aurora-cluster)

```yaml
# scheduler class for distributing the topology for execution
heron.class.scheduler: org.apache.heron.scheduler.aurora.AuroraScheduler

# launcher class for submitting and launching the topology
heron.class.launcher: org.apache.heron.scheduler.aurora.AuroraLauncher

# location of the core package
heron.package.core.uri: file:///home/vagrant/.heron/dist/heron-core.tar.gz

# location of java - pick it up from shell environment
heron.directory.sandbox.java.home: /usr/lib/jvm/java-1.8.0-openjdk-amd64/

# Invoke the IScheduler as a library directly
heron.scheduler.is.service: False
```

Additionally edit the `client.yaml` file and change the core uri to make it consistant.

```yaml
# location of the core package
heron.package.core.uri: file:///home/vagrant/.heron/dist/heron-core.tar.gz
```

### Important Step: Change folder name `aurora` to `devcluster`

Next you need to change the folder name of `/home/vagrant/.heron/conf/aurora` to
`/home/vagrant/.heron/conf/devcluster`. This is because the name of your aurora
cluster is `devcluster` as you noted in a previous step. You can do this with the
following commands

```bash
$ cd /home/vagrant/.heron/conf/
$ mv aurora devcluster
```

## Submitting Example Topology to Aurora cluster

Now you can submit a topology to the aurora cluster. this can be done with the following command.

```bash
$ heron submit devcluster/heronuser/devel --config-path ~/.heron/conf/ ~/.heron/examples/heron-api-examples.jar org.apache.heron.examples.api.ExclamationTopology ExclamationTopology
```

Now you should be able to see the topology in the Aurora UI ( http://192.168.33.7:8081/scheduler/heronuser ) .

![Heron topology](assets/aurora-local-topology-submitted.png)

### Understanding the parameters


below is a brief explanation on some of the important parameters that are used in this command. the first
parameter `devcluster/heronuser/devel` defines cluster, role and env ( env can have values `prod | devel | test | staging` ).
The cluster is the name of the aurora cluster which is `devcluster` in our case. You can give something like your
name for the role name and for env you need to choose from one of the env values.

`--config-path` points to the config folder. the program will automatically look for a folder with the cluster name.
This is why you had to change the name of the aurora conf folder to devcluster.

Now that everything is working you need to perform one last step to be able to see the typologies that you can see in Aurora UI in Heron UI.

## Associating new Aurora cluster into Heron UI

Heron UI uses information that is gets from the heron tracker when displaying the information in the heron UI interface.
So in-order to allow the Heron UI to show Aurora cluster information you need to modify configuration of the Heron tracker
so that it can identify the Aurora Cluster.

Heron Tracker configurations are located at `/home/vagrant/.herontools/conf` the configuration file is named `heron_tracker.yaml`.
By default you should see the following in the file

```yaml
statemgrs:
  -
    type: "file"
    name: "local"
    rootpath: "~/.herondata/repository/state/local"
    tunnelhost: "localhost"
  -
    type: "zookeeper"
    name: "localzk"
    hostport: "localhost:2181"
    rootpath: "/heron"
    tunnelhost: "localhost"
```

You can see that there already two entries. Before, you had to create paths in Zookeeper for `/heron/topologies` this is
because the entry named `localzk` in this file. If you remove this you will not need to create that path in Zookeeper.
Now all you have to is to add a new entry for the aurora cluster into this file ( lets comment out `localzk` ).
Then the file would look like below.

```yaml
statemgrs:
  -
    type: "file"
    name: "local"
    rootpath: "~/.herondata/repository/state/local"
    tunnelhost: "localhost"
  #-
   #type: "zookeeper"
   # name: "localzk"
   # hostport: "localhost:2181"
   # rootpath: "/heron"
   # tunnelhost: "localhost"
  -
    type: "zookeeper"
    name: "devcluster"
    hostport: "localhost:2181"
    rootpath: "/heronroot"
    tunnelhost: "localhost"
```

Now you can start Heron tracker and then Heron UI, Now you will be able to see the aurora cluster from the
Heron UI ( http://192.168.33.7:8889/topologies ) as below

```bash
$ heron-tracker
$ heron-ui
```

![Heron topology](assets/heron-ui-topology-submitted.png)
