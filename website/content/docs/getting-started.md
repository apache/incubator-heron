---
date: 2016-02-28T13:10:21-08:00
title: Getting Started
---

Run topologies locally using pre-compiled Heron binaries (Mac OSX, Ubuntu >= 14.04, Centos7)

### Step 1 - Download pre-compiled Heron binaries with install scripts

Navigate to [Twitter Heron Releases](https://github.com/twitter/heron/releases) and
download the following self extracting binary install scripts for your platform. 

* heron-client-install
* heron-tools-install

For example, if you want to download for Mac OSX (darwin), the 
corresponding binaries will be

* heron-client-install-\<version\>-darwin.sh
* heron-tools-install-\<version\>-darwin.sh

where \<version\> is the desired heron version.

Run the download self installing binary for heron client as follows
```bash
$ chmod +x heron-client-install-0.13.1-darwin.sh
$ ./heron-client-install-0.13.1-darwin.sh --user
Uncompressing......
Heron is now installed!
Make sure you have "/Users/$USER/bin" in your path.
```

Run the download self installing binary for heron tools as follows
```bash
$ chmod +x heron-tools-install-0.13.1-darwin.sh
$ ./heron-tools-install-0.13.1-darwin.sh --user
Uncompressing......
Heron Tools is now installed!
Make sure you have "/Users/$USER/bin" in your path.
```

### Step 2 - Launch an example topology

Launch an example [topology](../concepts/topologies) on a **local cluster** using submit:

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

### Step 5 - Explore activate, deactivate, and kill topology commands

```bash
$ heron activate local ExclamationTopology
$ heron deactivate local ExclamationTopology
$ heron kill local ExclamationTopology
```
Explore [managing topologies with Heron CLI](../operators/heron-cli)
and heron cli syntax. For example, to list the available commands:
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
  --config-path (a string; path to cluster config; default: "/Users/USERNAME/.heron/conf/<cluster>")
  --config-property (a string; a config property; default: [])
  --deploy-deactivated (a boolean; default: "false")
  --verbose (a boolean; default: "false")
```

### Step 6 - Explore other example topologies

**ExclamationTopology.java** | This is a basic example of a Heron topology.

**AckingTopology.java**  | This is a basic example of a Heron topology with acking enabled.

**MultiSpoutExclamationTopology.java** | This is a basic example of a Heron topology with multiple spouts.

**MultiStageAckingTopology.java** | This is three stage topology. Spout emits to bolt that feeds to another bolt. 

**TaskHookTopology.java** | This is a basic Task Hook Heron topology.

**CustomGroupingTopology.java** | This is a basic example of a Heron topology that implements custom grouping 

**ComponentJVMOptionsTopology.java** | This is a basic example of a Heron topology that supplies JVM options for each component

### Next Steps - Deploying or Developing

[Deploying Existing topologies](../operators/deployment/README) in clustered, scheduler-driven environments (Aurora, Mesos, Local)

[Developing Topologies](../concepts/architecture) with the Architecture of Heron

