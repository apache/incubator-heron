---
date: 2016-02-28T13:10:21-08:00
title: Getting Started - Local (Single Node)
---

Run topologies locally using pre-compiled Heron binaries (Mac OSX, Ubuntu >= 14.04, Centos7)

### Step 1 - Download Heron binaries with install scripts

Navigate to [Twitter Heron Releases](https://github.com/twitter/heron/releases) and
download the following self extracting binary install scripts for your platform. 

* heron-client-install
* heron-tools-install

For example, if you want to download for Mac OSX (darwin), the 
corresponding binaries will be

* heron-client-install-\<version\>-darwin.sh
* heron-tools-install-\<version\>-darwin.sh

where \<version\> is the desired heron version. For example, \<version\>=0.14.0

Run the download self installing binary for heron client using ```--user``` as follows
```bash
$ chmod +x heron-client-install-<version>-darwin.sh
$ ./heron-client-install-<version>-darwin.sh --user
Uncompressing......
Heron is now installed!
Make sure you have "/Users/$USER/bin" in your path.
```
To add ```/Users/$USER/bin``` to your path, run:
```bash
$ export PATH="$PATH:$HOME/bin"
```

Run the download self installing binary for heron tools using ```--user``` as follows
```bash
$ chmod +x heron-tools-install-<version>-darwin.sh
$ ./heron-tools-install-<version>-darwin.sh --user
Uncompressing......
Heron Tools is now installed!
Make sure you have "/Users/$USER/bin" in your path.
```

### Step 2 - Launch an example topology

Example topologies are installed with ```--user``` flag in ```~/.heron/examples```.  Launch an example [topology](../concepts/topologies) on **local cluster** using submit:

```bash
$ heron submit local ~/.heron/examples/heron-examples.jar com.twitter.heron.examples.ExclamationTopology ExclamationTopology
```

### Step 3 - Start Heron Tracker

Open a new terminal window and launch [heron-tracker](../operators/heron-tracker):
```bash
$ heron-tracker
... Running on port: 8888
... Using config file: /Users/USERNAME/.herontools/conf/localfilestateconf.yaml
```
In local browser, Heron tracker can be reached at http://localhost:8888


### Step 4 - Start Heron UI

Open a new terminal window and launch UI:
```bash
$ heron-ui
... Running on port: 8889
... Using tracker url: http://localhost:8888
```
In local browser, Heron UI is available at http://localhost:8889

### Step 5 - Explore topology management commands

```bash
$ heron activate local ExclamationTopology
$ heron deactivate local ExclamationTopology
$ heron kill local ExclamationTopology
```
Explore the [Heron CLI](../operators/heron-cli)
and the [topology lifecycle](../concepts/topologies#topology-lifecycle). To list the available CLI commands:
```bash
usage: heron <command> <options> ...

Available commands:
    activate           Activate a topology
    deactivate         Deactivate a topology
    help               Prints help for commands
    kill               Kill a topology
    restart            Restart a topology
    submit             Submit a topology
    version            Print version of heron-cli

For detailed documentation, go to http://heronstreaming.io
```

To invoke the help for submitting a topology:
```bash
$ heron help submit
usage: heron submit [options] cluster/[role]/[environ] topology-file-name topology-class-name [topology-args]

Required arguments:
  cluster/[role]/[env]  Cluster, role, and environ to run topology
  topology-file-name    Topology jar/tar/zip file
  topology-class-name   Topology class name

Optional arguments:
  --config-path (a string; path to cluster config; default: "/Users/$USER/.heron/conf")
  --config-property (key=value; a config key and its value; default: [])
  --deploy-deactivated (a boolean; default: "false")
  -D DEFINE             Define a system property to pass to java -D when
                        running main.
  --verbose (a boolean; default: "false")
```

### Step 6 - Explore other example topologies

The source code for the example topologies can be found at
[heron/examples/src/java/com/twitter/heron/examples](https://github.com/twitter/heron/tree/master/heron/examples/src/java/com/twitter/heron/examples).

```AckingTopology.java``` - a topology with acking enabled.

```ComponentJVMOptionsTopology.java``` - a topology that supplies JVM options for each component.

```CustomGroupingTopology.java``` - a topology that implements custom grouping. 

```ExclamationTopology.java``` - a spout emits random words to a bolt that adds an explanation mark.

```MultiSpoutExclamationTopology.java``` - a topology with multiple spouts.

```MultiStageAckingTopology.java``` - a three stage topology. A spout emits to bolt that feeds to another bolt. 

```TaskHookTopology.java``` - a topology that uses a task hook to subscribe for event notifications.

### Next Steps
[Upgrade Storm topologies](../upgrade-storm-to-heron) with simple POM.xml changes

[Deploy topologies](../operators/deployment) in clustered, scheduler-driven environments (Aurora, Mesos, Local)

[Develop topologies](../concepts/architecture) for the Heron Architecture

